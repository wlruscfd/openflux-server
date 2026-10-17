package avito

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/p1neappleXpress/OpenFlux/transport"
	"github.com/p1neappleXpress/OpenFlux/utils"
)

var baseURL = "https://code.avito.ru"

const (
	avitoHost   = "code.avito.ru"
	avitoUA     = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"
	shareDBType = "http://sharejs.org/types/textv1"
	roomBudget  = 16
	maxRooms    = 4
)

var sharedHTTP = sync.OnceValue(transport.ProtectedHTTPTransport)

var fleetRooms atomic.Int32

func init() { fleetRooms.Store(maxRooms) }

func SetFleetSize(keys int) {
	if keys < 1 {
		keys = 1
	}
	fleetRooms.Store(int32(max(1, min(maxRooms, roomBudget/keys))))
}

type AvitoConfig struct {
	WSHandshakeTimeout  time.Duration
	WSReadTimeout       time.Duration
	WSWriteTimeout      time.Duration
	WSPingInterval      time.Duration
	ReconnectMinDelay   time.Duration
	ReconnectMaxDelay   time.Duration
	ReconnectMultiplier float64
	HealthyAfter        time.Duration
	HTTPTimeout         time.Duration

	ChunkBytes int

	SendQueueSize int

	ReadBufferSize  int
	WriteBufferSize int
	MaxMessageBytes int64

	NumRooms        int
	RoomCreatePause time.Duration

	StatsInterval time.Duration
}

func DefaultAvitoConfig() AvitoConfig {
	return AvitoConfig{
		WSHandshakeTimeout:  15 * time.Second,
		WSReadTimeout:       90 * time.Second,
		WSWriteTimeout:      15 * time.Second,
		WSPingInterval:      25 * time.Second,
		ReconnectMinDelay:   500 * time.Millisecond,
		ReconnectMaxDelay:   30 * time.Second,
		ReconnectMultiplier: 1.6,
		HealthyAfter:        30 * time.Second,
		HTTPTimeout:         30 * time.Second,

		ChunkBytes:    256 * 1024,
		SendQueueSize: 256,

		ReadBufferSize:  32 << 10,
		WriteBufferSize: 32 << 10,
		MaxMessageBytes: 8 << 20,

		NumRooms:        int(fleetRooms.Load()),
		RoomCreatePause: 300 * time.Millisecond,

		StatsInterval: 30 * time.Second,
	}
}

var (
	errRoomGone = errors.New("avito: room does not exist")
	errRefused  = errors.New("refused")
	errNoRoom   = errors.New("avito: no connected room")
	errStopped  = errors.New("avito: transport stopped")
	errAuth     = errors.New("avito: cookies rejected")
)

func short(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

func jittered(d time.Duration) time.Duration {
	return d + time.Duration(rand.Int63n(int64(d/4)+1))
}

func createRoom(ctx context.Context, cookies string, slot int) (string, error) {
	ctx = transport.WithDialHint(ctx, transport.DialHint{Subject: fmt.Sprintf("room-%d", slot), Spread: true})
	jar, _ := cookiejar.New(nil)
	u, _ := url.Parse(baseURL)
	parseCookiesInto(jar, u, cookies)

	if !transport.WaitHostSlot(avitoHost, ctx.Done()) {
		return "", errStopped
	}
	client := &http.Client{Jar: jar, Timeout: 30 * time.Second, Transport: sharedHTTP()}
	req, _ := http.NewRequestWithContext(ctx, "POST", baseURL+"/web/1/code/room", bytes.NewReader(nil))
	req.Header.Set("User-Agent", avitoUA)
	req.Header.Set("Origin", baseURL)
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("POST room: %w", err)
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return "", fmt.Errorf("%w: status %d", errAuth, resp.StatusCode)
	}
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("POST room status %d: %s", resp.StatusCode, truncate(body, 200))
	}
	var result struct {
		Room struct {
			ID string `json:"id"`
		} `json:"room"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("parse room: %w", err)
	}
	if result.Room.ID == "" {
		return "", fmt.Errorf("empty room id")
	}
	return result.Room.ID, nil
}

func parseCookiesInto(jar http.CookieJar, u *url.URL, raw string) {
	for _, part := range strings.Split(raw, ";") {
		part = strings.TrimSpace(part)
		if eq := strings.IndexByte(part, '='); eq > 0 {
			jar.SetCookies(u, []*http.Cookie{{
				Name: part[:eq], Value: part[eq+1:], Path: "/",
			}})
		}
	}
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}

type avitoWS struct {
	idx     int
	roomID  string
	cookies atomic.Value
	config  AvitoConfig
	ctx     context.Context

	conn    *websocket.Conn
	writeMu sync.Mutex
	connID  string
	ver     int
	docLen  int
	seq     int

	connected atomic.Bool
	onData    func([]byte)
	sendQueue chan []byte
	ackCh     chan map[string]interface{}
	stats     *channelStats
}

type channelStats struct {
	idx        int
	roomID     string
	opsSent    atomic.Uint64
	opsRecv    atomic.Uint64
	bytesSent  atomic.Uint64
	bytesRecv  atomic.Uint64
	reconnects atomic.Uint64
	errors     atomic.Uint64
}

func (w *avitoWS) cookieHeader() string {
	s, _ := w.cookies.Load().(string)
	return s
}

func (w *avitoWS) dial() (*websocket.Conn, error) {
	wsURL := strings.Replace(baseURL, "https://", "wss://", 1) + "/sharedb"
	wsURL = strings.Replace(wsURL, "http://", "ws://", 1)

	header := http.Header{}
	header.Set("User-Agent", avitoUA)
	header.Set("Origin", baseURL)
	header.Set("Cookie", w.cookieHeader())

	if !transport.WaitHostSlot(avitoHost, w.ctx.Done()) {
		return nil, errStopped
	}
	dialer := websocket.Dialer{
		NetDialContext:   sharedHTTP().DialContext,
		HandshakeTimeout: w.config.WSHandshakeTimeout,
		ReadBufferSize:   w.config.ReadBufferSize,
		WriteBufferSize:  w.config.WriteBufferSize,
	}
	conn, _, err := dialer.DialContext(w.ctx, wsURL, header)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	return conn, nil
}

func (w *avitoWS) writeJSON(v interface{}) error {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	if w.conn == nil {
		return fmt.Errorf("ws not connected")
	}
	w.conn.SetWriteDeadline(time.Now().Add(w.config.WSWriteTimeout))
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if err := w.conn.WriteMessage(websocket.TextMessage, data); err != nil {
		w.conn.Close()
		return err
	}
	return nil
}

func (w *avitoWS) readJSON(v interface{}) error {
	w.conn.SetReadDeadline(time.Now().Add(w.config.WSReadTimeout))
	_, raw, err := w.conn.ReadMessage()
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, v)
}

func (w *avitoWS) subscribe() error {
	var init struct {
		ID string `json:"id"`
	}
	if err := w.readJSON(&init); err != nil {
		return fmt.Errorf("init: %w", err)
	}
	w.connID = init.ID

	if err := w.writeJSON(map[string]interface{}{
		"a": "s", "c": "docs", "d": w.roomID, "v": nil,
	}); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	var sub map[string]interface{}
	if err := w.readJSON(&sub); err != nil {
		return fmt.Errorf("subscribe reply: %w", err)
	}

	if errMsg, ok := sub["error"].(map[string]interface{}); ok {
		return fmt.Errorf("%w: subscribe: %v", errRefused, errMsg)
	}

	doc, _ := sub["data"].(map[string]interface{})
	if doc == nil {
		doc = sub
	}
	if v, ok := doc["v"].(float64); ok {
		w.ver = int(v)
	}
	data, _ := doc["data"].(string)
	w.docLen = len(data)
	w.seq = 1

	if doc["type"] == nil {
		if err := w.writeJSON(map[string]interface{}{
			"a": "op", "c": "docs", "d": w.roomID, "v": 0,
			"src": w.connID, "seq": w.seq,
			"create": map[string]interface{}{"type": shareDBType, "data": ""},
		}); err != nil {
			return fmt.Errorf("create doc: %w", err)
		}
		var reply map[string]interface{}
		if err := w.readJSON(&reply); err != nil {
			return fmt.Errorf("create reply: %w", err)
		}
		if errMsg := reply["error"]; errMsg != nil {
			return fmt.Errorf("create error: %v", errMsg)
		}
		w.ver = 1
		w.docLen = 0
		w.seq = 2
	}

	return nil
}

func (w *avitoWS) run() {
	delay := w.config.ReconnectMinDelay
	for w.ctx.Err() == nil {
		started := time.Now()
		err := w.connectAndServe()
		w.connected.Store(false)
		if w.ctx.Err() != nil {
			return
		}
		utils.Debugf("[AVITO] ws error (%s): %v", short(w.roomID), err)
		w.stats.reconnects.Add(1)
		if time.Since(started) >= w.config.HealthyAfter {
			delay = w.config.ReconnectMinDelay
		}
		if !sleepCtx(w.ctx, jittered(delay)) {
			return
		}
		delay = min(time.Duration(float64(delay)*w.config.ReconnectMultiplier), w.config.ReconnectMaxDelay)
	}
}

func (w *avitoWS) pingLoop(conn *websocket.Conn, done <-chan struct{}) {
	if w.config.WSPingInterval <= 0 {
		return
	}
	tick := time.NewTicker(jittered(w.config.WSPingInterval))
	defer tick.Stop()
	for {
		select {
		case <-done:
			return
		case <-w.ctx.Done():
			return
		case <-tick.C:
			if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(w.config.WSWriteTimeout)); err != nil {
				conn.Close()
				return
			}
		}
	}
}

func (w *avitoWS) connectAndServe() error {
	conn, err := w.dial()
	if err != nil {
		return err
	}
	unhook := context.AfterFunc(w.ctx, func() { conn.Close() })
	defer unhook()
	w.writeMu.Lock()
	w.conn = conn
	w.writeMu.Unlock()
	finished := make(chan struct{})
	defer func() {
		close(finished)
		w.writeMu.Lock()
		w.conn = nil
		w.writeMu.Unlock()
		conn.Close()
	}()

	conn.SetReadLimit(w.config.MaxMessageBytes)
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(w.config.WSReadTimeout))
	})

	if err := w.subscribe(); err != nil {
		return err
	}
	w.connected.Store(true)
	w.ackCh = make(chan map[string]interface{}, 1)
	utils.Debugf("[AVITO] WS ready: %s ver=%d docLen=%d", short(w.roomID), w.ver, w.docLen)

	go w.pingLoop(conn, finished)

	readDone := make(chan error, 1)
	go func() {
		readDone <- w.readLoop()
	}()
	sendDone := make(chan error, 1)
	go func() {
		sendDone <- w.sendLoop()
	}()

	select {
	case err := <-readDone:
		return err
	case err := <-sendDone:
		return err
	case <-w.ctx.Done():
		return errStopped
	}
}

func (w *avitoWS) readLoop() error {
	for {
		w.conn.SetReadDeadline(time.Now().Add(w.config.WSReadTimeout))
		_, raw, err := w.conn.ReadMessage()
		if err != nil {
			return err
		}
		var msg map[string]interface{}
		if json.Unmarshal(raw, &msg) != nil {
			continue
		}

		a, _ := msg["a"].(string)
		src, _ := msg["src"].(string)

		switch {
		case a == "op" && src != "" && src != w.connID:
			w.handlePeerOp(msg)
		default:
			select {
			case w.ackCh <- msg:
			case <-w.ctx.Done():
				return errStopped
			}
		}
	}
}

func (w *avitoWS) handlePeerOp(msg map[string]interface{}) {
	ops, ok := msg["op"].([]interface{})
	if !ok || len(ops) == 0 {
		return
	}
	for _, op := range ops {
		s, ok := op.(string)
		if !ok || len(s) == 0 {
			continue
		}
		decoded, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			continue
		}
		if w.onData != nil {
			w.onData(decoded)
		}
		w.stats.opsRecv.Add(1)
		w.stats.bytesRecv.Add(uint64(len(decoded)))
	}
	if v, ok := msg["v"].(float64); ok {
		newVer := int(v) + 1
		if newVer > w.ver {
			w.ver = newVer
		}
	}
}

func (w *avitoWS) sendLoop() error {
	for {
		select {
		case <-w.ctx.Done():
			return errStopped
		case data := <-w.sendQueue:
			if err := w.sendOp(data); err != nil {
				return err
			}
		}
	}
}

func (w *avitoWS) sendOp(data []byte) error {
	encoded := base64.StdEncoding.EncodeToString(data)

	var op []interface{}
	if w.docLen > 0 {
		op = []interface{}{map[string]int{"d": w.docLen}, encoded}
	} else {
		op = []interface{}{encoded}
	}

	if err := w.writeJSON(map[string]interface{}{
		"a": "op", "c": "docs", "d": w.roomID,
		"v": w.ver, "src": w.connID, "seq": w.seq, "op": op,
	}); err != nil {
		return err
	}

	select {
	case reply := <-w.ackCh:
		if errMsg := reply["error"]; errMsg != nil {
			w.stats.errors.Add(1)
			return fmt.Errorf("op error: %v", errMsg)
		}
		if v, ok := reply["v"].(float64); ok {
			w.ver = int(v) + 1
		} else {
			w.ver++
		}
	case <-w.ctx.Done():
		return errStopped
	}

	w.seq++
	w.docLen = len(encoded)
	w.stats.opsSent.Add(1)
	w.stats.bytesSent.Add(uint64(len(data)))
	return nil
}

func (w *avitoWS) Send(data []byte) error {
	select {
	case w.sendQueue <- data:
		return nil
	case <-w.ctx.Done():
		return errStopped
	}
}

type AvitoTransport struct {
	*transport.BaseTransport

	cookiesMu sync.RWMutex
	cookies   string
	roomIDs   []string
	config    AvitoConfig
	isClient  bool

	roomsMu    sync.Mutex
	roomList   string
	onRoomList func(packed string)

	ctx    context.Context
	cancel context.CancelFunc

	txWss []*avitoWS
	rxWss []*avitoWS
	rr    atomic.Uint32

	statsStart time.Time
}

func NewAvitoTransport(rawURL string, cfg transport.TransportConfig, cookies string, isClient bool) *AvitoTransport {
	ctx, cancel := context.WithCancel(context.Background())
	t := &AvitoTransport{
		BaseTransport: transport.NewBaseTransport(cfg),
		config:        DefaultAvitoConfig(),
		cookies:       cookies,
		isClient:      isClient,
		ctx:           ctx,
		cancel:        cancel,
		statsStart:    time.Now(),
	}
	t.roomIDs = parseRoomList(rawURL)
	return t
}

func (t *AvitoTransport) currentCookies() string {
	t.cookiesMu.RLock()
	defer t.cookiesMu.RUnlock()
	return t.cookies
}

func parseRoomList(rawURL string) []string {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return nil
	}
	if u, err := url.Parse(rawURL); err == nil {
		q := u.Query()
		if packed := q.Get("rooms"); packed != "" {
			if ids, err := unpackRooms(packed); err == nil {
				return ids
			}
		}
	}
	if ids, err := unpackRooms(rawURL); err == nil {
		return ids
	}
	return nil
}

func packRooms(ids []string) string {
	raw, _ := json.Marshal(ids)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func unpackRooms(s string) ([]string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	var ids []string
	if err := json.Unmarshal(raw, &ids); err != nil {
		return nil, err
	}
	return ids, nil
}

func (t *AvitoTransport) Start() error {
	if err := t.BaseTransport.Start(); err != nil {
		return err
	}

	allIDs, err := t.enterRooms()
	if err != nil {
		return err
	}
	if t.ctx.Err() != nil {
		return errStopped
	}

	half := len(allIDs) / 2
	if half == 0 {
		half = 1
	}
	var txIDs, rxIDs []string
	if t.isClient {
		txIDs = allIDs[half:]
		rxIDs = allIDs[:half]
	} else {
		txIDs = allIDs[:half]
		rxIDs = allIDs[half:]
	}

	mkWS := func(ids []string, onData func([]byte)) []*avitoWS {
		wss := make([]*avitoWS, len(ids))
		for i, id := range ids {
			ws := &avitoWS{
				idx:       i,
				roomID:    id,
				config:    t.config,
				ctx:       t.ctx,
				sendQueue: make(chan []byte, t.config.SendQueueSize),
				stats:     &channelStats{idx: i, roomID: id},
			}
			ws.cookies.Store(t.currentCookies())
			ws.onData = onData
			wss[i] = ws
		}
		return wss
	}

	t.txWss = mkWS(txIDs, nil)
	t.rxWss = mkWS(rxIDs, t.CallReceive)

	for _, ws := range t.txWss {
		go ws.run()
	}
	for _, ws := range t.rxWss {
		go ws.run()
	}

	utils.Debugf("[AVITO] transport started: %d tx + %d rx rooms", len(t.txWss), len(t.rxWss))
	t.SetConnected(true)

	go t.statsLoop()
	return nil
}

func (t *AvitoTransport) enterRooms() ([]string, error) {
	if len(t.roomIDs) > 0 {
		utils.Infof("[AVITO] joining %d rooms", len(t.roomIDs))
		t.setRoomList(t.roomIDs)
		return t.roomIDs, nil
	}
	if t.isClient {
		return nil, fmt.Errorf("avito client: the url must contain the room list")
	}

	total := t.config.NumRooms * 2
	ids := make([]string, 0, total)
	for i := 0; i < total; i++ {
		if t.ctx.Err() != nil {
			return nil, errStopped
		}
		var roomID string
		var lastErr error
		for attempt := 0; attempt < 5; attempt++ {
			roomID, lastErr = createRoom(t.ctx, t.currentCookies(), i)
			if lastErr == nil || errors.Is(lastErr, errStopped) {
				break
			}
			if errors.Is(lastErr, errAuth) {
				if !transport.DialHookInstalled() {
					break
				}
				transport.ReportDial(transport.WithDialHint(t.ctx, transport.DialHint{Subject: fmt.Sprintf("room-%d", i)}), lastErr)
			}
			wait := t.config.RoomCreatePause * time.Duration(1<<uint(attempt))
			if wait > 15*time.Second {
				wait = 15 * time.Second
			}
			utils.Debugf("[AVITO] room %d attempt %d failed: %v (wait %v)", i+1, attempt+1, lastErr, wait)
			if !sleepCtx(t.ctx, jittered(wait)) {
				return nil, errStopped
			}
		}
		if errors.Is(lastErr, errAuth) || errors.Is(lastErr, errStopped) {
			return nil, lastErr
		}
		if roomID == "" {
			utils.Debugf("[AVITO] room %d skipped: %v", i+1, lastErr)
			continue
		}
		ids = append(ids, roomID)
		utils.Debugf("[AVITO] created room %d/%d: %s", i+1, total, short(roomID))
		if i < total-1 {
			if !sleepCtx(t.ctx, t.config.RoomCreatePause) {
				return nil, errStopped
			}
		}
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("could not create any room")
	}

	t.setRoomList(ids)
	return ids, nil
}

func (t *AvitoTransport) RoomList() string {
	t.roomsMu.Lock()
	defer t.roomsMu.Unlock()
	return t.roomList
}

func (t *AvitoTransport) OnRoomList(f func(packed string)) {
	t.roomsMu.Lock()
	t.onRoomList = f
	t.roomsMu.Unlock()
}

func (t *AvitoTransport) setRoomList(ids []string) {
	packed := packRooms(ids)
	t.roomsMu.Lock()
	changed := packed != t.roomList
	t.roomList = packed
	f := t.onRoomList
	t.roomsMu.Unlock()
	if changed && f != nil {
		f(packed)
	}
}

func (t *AvitoTransport) Stop() error {
	t.cancel()
	t.SetConnected(false)
	return t.BaseTransport.Stop()
}

func (t *AvitoTransport) Send(data []byte) error {
	ws := t.pickTxRoom()
	if ws == nil {
		return errNoRoom
	}
	return ws.Send(data)
}

func (t *AvitoTransport) pickTxRoom() *avitoWS {
	n := len(t.txWss)
	if n == 0 {
		return nil
	}
	start := int(t.rr.Add(1))
	for i := 0; i < n; i++ {
		ws := t.txWss[(start+i)%n]
		if ws.connected.Load() {
			return ws
		}
	}
	return nil
}

func (t *AvitoTransport) Receive(callback func([]byte)) {
	t.BaseTransport.Receive(callback)
}

func (t *AvitoTransport) IsConnected() bool {
	if !t.BaseTransport.IsConnected() {
		return false
	}
	for _, ws := range t.txWss {
		if ws.connected.Load() {
			return true
		}
	}
	for _, ws := range t.rxWss {
		if ws.connected.Load() {
			return true
		}
	}
	return false
}

func (t *AvitoTransport) allWS() []*avitoWS {
	out := make([]*avitoWS, 0, len(t.txWss)+len(t.rxWss))
	out = append(out, t.txWss...)
	out = append(out, t.rxWss...)
	return out
}

func (t *AvitoTransport) Stats() transport.TransportStats {
	var sent, recv, bytesSent, bytesRecv, reconnects uint64
	for _, ws := range t.allWS() {
		sent += ws.stats.opsSent.Load()
		recv += ws.stats.opsRecv.Load()
		bytesSent += ws.stats.bytesSent.Load()
		bytesRecv += ws.stats.bytesRecv.Load()
		reconnects += ws.stats.reconnects.Load()
	}
	base := t.BaseTransport.Stats()
	return transport.TransportStats{
		BytesSent:     bytesSent,
		BytesReceived: bytesRecv,
		PacketsSent:   sent,
		PacketsRecv:   recv,
		Reconnects:    reconnects,
		Connected:     t.IsConnected(),
		Uptime:        base.Uptime,
	}
}

func (t *AvitoTransport) statsLoop() {
	tick := time.NewTicker(t.config.StatsInterval)
	defer tick.Stop()

	var lastBS, lastBR uint64
	for {
		select {
		case <-t.ctx.Done():
			return
		case <-tick.C:
			var bs, br uint64
			up := 0
			for _, ws := range t.allWS() {
				bs += ws.stats.bytesSent.Load()
				br += ws.stats.bytesRecv.Load()
				if ws.connected.Load() {
					up++
				}
			}
			secs := t.config.StatsInterval.Seconds()
			utils.Debugf("[AVITO-TOTAL] tx=%.1f KB/s rx=%.1f KB/s rooms=%d/%d uptime=%v",
				float64(bs-lastBS)/secs/1024, float64(br-lastBR)/secs/1024,
				up, len(t.txWss)+len(t.rxWss), time.Since(t.statsStart).Round(time.Second))
			lastBS, lastBR = bs, br
		}
	}
}

func CreateRoomList(ctx context.Context, cookies string) (string, error) {
	cfg := DefaultAvitoConfig()
	total := cfg.NumRooms * 2
	ids := make([]string, 0, total)
	for i := 0; i < total; i++ {
		id, err := createRoom(ctx, cookies, i)
		if err != nil {
			if len(ids) > 0 {
				break
			}
			return "", err
		}
		ids = append(ids, id)
		if i < total-1 {
			if !sleepCtx(ctx, cfg.RoomCreatePause) {
				return "", errStopped
			}
		}
	}
	return packRooms(ids), nil
}

func (t *AvitoTransport) FetchCookies() (map[string]string, error) {
	out := make(map[string]string)
	for _, part := range strings.Split(t.currentCookies(), ";") {
		part = strings.TrimSpace(part)
		if eq := strings.IndexByte(part, '='); eq > 0 {
			out[part[:eq]] = part[eq+1:]
		}
	}
	return out, nil
}

func (t *AvitoTransport) ApplyCookies(values map[string]string) error {
	if len(values) == 0 {
		return nil
	}
	parts := make([]string, 0, len(values))
	for k, v := range values {
		parts = append(parts, k+"="+v)
	}
	joined := strings.Join(parts, "; ")
	t.cookiesMu.Lock()
	t.cookies = joined
	t.cookiesMu.Unlock()
	for _, ws := range t.allWS() {
		ws.cookies.Store(joined)
		ws.writeMu.Lock()
		if ws.conn != nil {
			ws.conn.Close()
		}
		ws.writeMu.Unlock()
	}
	return nil
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
