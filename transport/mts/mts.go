package mts

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	mrand "math/rand"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"universal-bypass-tool/transport"
	"universal-bypass-tool/utils"
)

const (
	mtsOrigin    = "https://my.mts-link.ru"
	mtsReserveWS = "wss://wsboard2.mts-link.ru/boards"
	mtsUA        = "Mozilla/5.0 (Linux; Android 15; Pixel 9) AppleWebKit/537.36 " +
		"(KHTML, like Gecko) Chrome/153.0.0.0 Mobile Safari/537.36"

	mtsPingInterval  = 60 * time.Second
	mtsReadDeadline  = 90 * time.Second
	mtsWriteDeadline = 10 * time.Second
	mtsDialTimeout   = 15 * time.Second
	mtsHTTPTimeout   = 15 * time.Second
	mtsMaxAttempt    = 10

	// The board relays cursor frames untouched and a 60KB payload has been observed
	// arriving intact, so batches stay well under that: one frame per burst instead of
	// one frame per packet is where nearly all of the throughput comes from.
	mtsDefaultBatchBytes = 48 * 1024
	mtsDefaultBatchCount = 64
	mtsLinger            = 2 * time.Millisecond

	// mtsSendWait bounds how long Send blocks on a full queue. The tunnel ignores Send's
	// error (tunnel/endpoint.go hands packets to a fire-and-forget callback), so returning
	// an error here would silently drop a packet; a short wait turns a transient burst
	// into a little latency instead of a loss the TCP stack only notices on retransmit.
	mtsSendWait = 250 * time.Millisecond

	// mtsStashBytes caps packets carried across a reconnect. gvisor retransmits what it
	// must, so this is only a latency shortcut, and it has to stay bounded: managed mode
	// runs one transport per key, so per-transport buffers are multiplied by the fleet size.
	mtsStashBytes = 64 * 1024

	// mtsCursorY is a constant companion to the base64 payload riding in cursorPosition.x.
	// The board app never reads y back for guests, so it only has to look like a real sample.
	mtsCursorY = 123.0
)

type mtsInfo struct {
	boardUID   string
	token      string
	clientUID  string
	prefix     string
	signature  string
	appDomain  string
	wsDomain   string
	reserveWs  string
	pod        string
	reservePod string
	temporary  bool
	accessTok  string
	guestName  string
}

type mtsSession struct {
	Info    mtsInfo
	Conn    *websocket.Conn
	writeMu sync.Mutex

	sessionUID atomic.Pointer[string]
}

// Transport carries tunnel packets as MTS Link Boards "fast/view" cursor updates.
//
// The board is opened anonymously through a share link: the HTML page embeds a guest token,
// a client UID and a short-lived JWT signature, so no login and no CAPTCHA is involved.
// Every page load mints a fresh guest identity, which is what makes reconnects cheap.
//
// Outgoing packets are coalesced with transport.EncodeBatch and sent as one base64 cursor
// frame; the reader accepts both a batch frame and a bare packet, so a peer that still
// sends one packet per cursor update stays readable.
type Transport struct {
	*transport.BaseTransport

	url string

	session atomic.Pointer[mtsSession]

	onDataMu sync.RWMutex
	onData   func([]byte)

	closeOnce sync.Once
	done      chan struct{}

	wakeMu   sync.Mutex
	wakeChan chan struct{}

	out     chan []byte
	started atomic.Bool

	batchBytes int

	// slowCallback accumulates time spent inside the receive callback, for verbose diagnostics.
	slowCallback atomic.Int64
	batchCount   int

	stashMu  sync.Mutex
	stash    [][]byte
	stashLen int
}

func NewTransport(rawURL string, config transport.TransportConfig) *Transport {
	return &Transport{
		BaseTransport: transport.NewBaseTransport(config),
		url:           rawURL,
		done:          make(chan struct{}),
		// Deliberately the configured depth and no deeper: at MTU-sized packets a queue is
		// ~1.4MB, and nodeagent runs one of these per key. Send waits for room instead of
		// growing the buffer.
		out:        make(chan []byte, config.MaxQueueSize),
		batchBytes: envInt("OPENFLUX_MTS_BATCH_BYTES", mtsDefaultBatchBytes),
		batchCount: envInt("OPENFLUX_MTS_BATCH_COUNT", mtsDefaultBatchCount),
	}
}

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func (t *Transport) Start() error {
	if err := t.BaseTransport.Start(); err != nil {
		return err
	}
	boardUID := extractBoardUID(t.url)
	if boardUID == "" {
		return fmt.Errorf("mts: no board UID in URL %q", t.url)
	}

	t.closeOnce = sync.Once{}
	t.done = make(chan struct{})
	utils.SafeGo("mts.writer", t.writerLoop)
	utils.SafeGo("mts.connect", func() { t.connectLoop(boardUID) })
	return nil
}

func (t *Transport) Stop() error {
	t.closeOnce.Do(func() { close(t.done) })
	if s := t.session.Load(); s != nil && s.Conn != nil {
		s.Conn.Close()
	}
	t.SetConnected(false)
	return t.BaseTransport.Stop()
}

func (t *Transport) ForceReconnect() {
	if s := t.session.Load(); s != nil && s.Conn != nil {
		s.Conn.Close()
		return
	}
	t.wakeMu.Lock()
	wake := t.wakeChan
	t.wakeChan = nil
	t.wakeMu.Unlock()
	if wake != nil {
		close(wake)
	}
}

func (t *Transport) Send(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	timer := time.NewTimer(mtsSendWait)
	defer timer.Stop()
	select {
	case t.out <- cp:
		return nil
	case <-t.done:
		return fmt.Errorf("mts: closed")
	case <-timer.C:
		return fmt.Errorf("mts: queue full")
	}
}

func (t *Transport) Receive(cb func([]byte)) {
	t.onDataMu.Lock()
	t.onData = cb
	t.onDataMu.Unlock()
}

func (t *Transport) IsConnected() bool {
	return t.BaseTransport.IsConnected()
}

// writerLoop is owned by the transport, not by a session, so packets accepted right before a
// socket dies are still written after the reconnect instead of vanishing with the old session.
func (t *Transport) writerLoop() {
	t.started.Store(true)
	defer t.started.Store(false)

	batch := make([][]byte, 0, t.batchCount)
	size := 0

	reset := func() {
		batch = batch[:0]
		size = 0
	}
	add := func(p []byte) {
		batch = append(batch, p)
		size += len(p) + 2
	}

	for {
		select {
		case <-t.done:
			return
		case p := <-t.out:
			add(p)
		}

		// Stashed packets from a previous connection go first: they are the oldest. Only the
		// ones that fit this batch are taken - dropping the rest would lose them silently.
		t.stashMu.Lock()
		taken := 0
		for taken < len(t.stash) && len(batch) < t.batchCount && size < t.batchBytes {
			add(t.stash[taken])
			t.stashLen -= len(t.stash[taken])
			taken++
		}
		if taken > 0 {
			t.stash = append([][]byte(nil), t.stash[taken:]...)
		}
		t.stashMu.Unlock()

	drain:
		for len(batch) < t.batchCount && size < t.batchBytes {
			select {
			case p := <-t.out:
				add(p)
			default:
				break drain
			}
		}

		// A short linger is what actually turns a packet-per-cursor-update sender into one
		// frame per burst; without it every isolated packet pays a full round trip alone.
		if len(batch) < t.batchCount && size < t.batchBytes {
			timer := time.NewTimer(mtsLinger)
			select {
			case p := <-t.out:
				add(p)
			case <-timer.C:
			case <-t.done:
				timer.Stop()
				return
			}
			timer.Stop()
		drainAgain:
			for len(batch) < t.batchCount && size < t.batchBytes {
				select {
				case p := <-t.out:
					add(p)
				default:
					break drainAgain
				}
			}
		}

		if len(batch) == 0 {
			reset()
			continue
		}
		t.flush(batch, size)
		reset()
	}
}

func (t *Transport) flush(batch [][]byte, size int) {
	s := t.session.Load()
	if s == nil || s.Conn == nil {
		t.stashPackets(batch, size)
		return
	}
	frame := transport.EncodeBatch(batch)
	if err := t.sendCursor(s, frame); err != nil {
		utils.Debugf("[MTS] cursor send: %v", err)
		// Without this the writer keeps pulling packets and failing them one by one against a
		// dead socket: the tunnel would see nothing but retransmits and no reconnect.
		t.stashPackets(batch, size)
		t.ForceReconnect()
		return
	}
	t.RecordSend(size)
	if utils.IsVerbose() {
		utils.Debugf("[MTS->] batch packets=%d bytes=%d", len(batch), size)
	}
}

func (t *Transport) stashPackets(batch [][]byte, size int) {
	t.stashMu.Lock()
	defer t.stashMu.Unlock()
	for _, p := range batch {
		if t.stashLen+len(p) > mtsStashBytes {
			return
		}
		t.stash = append(t.stash, p)
		t.stashLen += len(p)
	}
}

func extractBoardUID(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	path := strings.Trim(u.Path, "/")
	parts := strings.Split(path, "/")
	for i := 0; i+1 < len(parts); i++ {
		if parts[i] == "board" && parts[i+1] != "" {
			return parts[i+1]
		}
	}
	if len(parts) > 0 {
		last := parts[len(parts)-1]
		if uuidRe.MatchString(last) {
			return last
		}
	}
	return u.Query().Get("boardUID")
}

var uuidRe = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)

func randomGuestName() string {
	var b [3]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("guest_%06x", time.Now().UnixNano()&0xffffff)
	}
	return "guest_" + hex.EncodeToString(b[:])
}

func jsStringField(html, name string) string {
	re := regexp.MustCompile(name + `\s*:\s*"([^"]*)"`)
	m := re.FindStringSubmatch(html)
	if len(m) != 2 {
		return ""
	}
	return m[1]
}

func jsBoolField(html, name string) bool {
	re := regexp.MustCompile(name + `\s*:\s*(true|false)`)
	m := re.FindStringSubmatch(html)
	if len(m) != 2 {
		return false
	}
	return m[1] == "true"
}

// fetchGuestSession loads the share page and scrapes the anonymous guest identity out of
// the inline baseInfo object the board app boots from.
func (t *Transport) fetchGuestSession(boardUID string) (mtsInfo, error) {
	jar, _ := cookiejar.New(nil)
	client := &http.Client{
		Jar:     jar,
		Timeout: mtsHTTPTimeout,
		Transport: &http.Transport{
			DialContext:           transport.ProtectedDialer().DialContext,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: mtsHTTPTimeout,
		},
	}

	pageURL := fmt.Sprintf("%s/boards/board/%s", mtsOrigin, boardUID)
	req, err := http.NewRequest("GET", pageURL, nil)
	if err != nil {
		return mtsInfo{}, err
	}
	req.Header.Set("User-Agent", mtsUA)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Accept-Language", "ru")
	resp, err := client.Do(req)
	if err != nil {
		return mtsInfo{}, err
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
	if resp.StatusCode != 200 {
		return mtsInfo{}, fmt.Errorf("board page status %d", resp.StatusCode)
	}
	html := string(body)

	info := mtsInfo{
		boardUID:  boardUID,
		token:     jsStringField(html, "token"),
		clientUID: jsStringField(html, "clientUID"),
		prefix:    jsStringField(html, "fePrefix"),
		signature: jsStringField(html, "signature"),
		appDomain: jsStringField(html, "appDomain"),
		wsDomain:  jsStringField(html, "wsDomain"),
		reserveWs: jsStringField(html, "reserveWsDomain"),
		accessTok: jsStringField(html, "boardAccessToken"),
		temporary: jsBoolField(html, "temporary"),
		guestName: randomGuestName(),
	}
	if info.boardUID == "" {
		if m := uuidRe.FindString(html); m != "" {
			info.boardUID = m
		}
	}
	if info.boardUID == "" {
		return mtsInfo{}, fmt.Errorf("board UID missing from page")
	}
	if info.token == "" || info.clientUID == "" || info.signature == "" {
		return mtsInfo{}, fmt.Errorf("guest identity missing from page (token=%d clientUID=%d signature=%d)",
			len(info.token), len(info.clientUID), len(info.signature))
	}
	if info.appDomain == "" {
		info.appDomain = "my.mts-link.ru/boards"
	}
	if info.wsDomain == "" {
		info.wsDomain = mtsReserveWS
	}
	if info.reserveWs == "" {
		info.reserveWs = mtsReserveWS
	}

	pods, err := t.fetchPods(client, info)
	if err != nil {
		utils.Debugf("[MTS] pod discovery failed (%v), falling back to pod 1", err)
	}
	info.pod = pods.pod
	info.reservePod = pods.reservePod

	return info, nil
}

type podInfo struct {
	pod        string
	reservePod string
}

func (t *Transport) fetchPods(client *http.Client, info mtsInfo) (podInfo, error) {
	payload, err := json.Marshal(map[string]string{"boardUID": info.boardUID})
	if err != nil {
		return podInfo{}, err
	}
	u := "https://" + strings.TrimSuffix(info.appDomain, "/") + "/api/v2/board/pod/get"
	req, err := http.NewRequest("POST", u, bytes.NewReader(payload))
	if err != nil {
		return podInfo{}, err
	}
	req.Header.Set("User-Agent", mtsUA)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Language", "ru")
	req.Header.Set("Origin", mtsOrigin)
	req.Header.Set("Referer", fmt.Sprintf("%s/boards/board/%s", mtsOrigin, info.boardUID))
	resp, err := client.Do(req)
	if err != nil {
		return podInfo{}, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode != 200 {
		return podInfo{}, fmt.Errorf("pod status %d", resp.StatusCode)
	}
	var out struct {
		Status     string `json:"status"`
		Pod        string `json:"pod"`
		ReservePod string `json:"reservePod"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return podInfo{}, err
	}
	if out.Status != "success" && out.Pod == "" {
		return podInfo{}, fmt.Errorf("pod status %q", out.Status)
	}
	if out.Pod == "" {
		out.Pod = "1"
	}
	if out.ReservePod == "" {
		out.ReservePod = "1"
	}
	return podInfo{pod: out.Pod, reservePod: out.ReservePod}, nil
}

// connectLoop re-scrapes a fresh guest identity on every attempt. Guest tokens are cheap
// and per-load, so a stale one is never worth reusing after a disconnect.
func (t *Transport) connectLoop(boardUID string) {
	attempt := 0
	for {
		select {
		case <-t.done:
			return
		default:
		}

		t.EmitEvent(transport.EventConnecting, strconv.Itoa(attempt+1))

		info, err := t.fetchGuestSession(boardUID)
		established := false
		if err != nil {
			utils.Debugf("[MTS] attempt %d: guest fetch failed: %v", attempt+1, err)
		} else {
			utils.Debugf("[MTS] attempt %d: guest clientUID=%s pod=%q reserve=%q",
				attempt+1, shortStr(info.clientUID, 8), info.pod, info.reservePod)
			established, err = t.connectAndServe(attempt, info)
		}
		t.SetConnected(false)
		if established {
			// A session that ran and then ended is a reconnect, no matter how clean the drop was.
			t.RecordReconnect()
		}

		delay := reconnectBackoff(attempt)
		cause := "connection closed"
		if err != nil {
			cause = strings.ReplaceAll(err.Error(), "\n", " ")
			utils.Debugf("[MTS] attempt %d failed: %v", attempt+1, err)
		}
		t.EmitEvent(transport.EventRetrying, fmt.Sprintf("%d|%d|%s|%s", attempt+1, int(delay.Seconds()), "mts_error", cause))

		wake := make(chan struct{})
		t.wakeMu.Lock()
		t.wakeChan = wake
		t.wakeMu.Unlock()

		select {
		case <-t.done:
			return
		case <-time.After(delay):
		case <-wake:
			utils.Debugf("[MTS] backoff wait cut short by ForceReconnect")
		}

		t.wakeMu.Lock()
		if t.wakeChan == wake {
			t.wakeChan = nil
		}
		t.wakeMu.Unlock()

		attempt++
		if attempt > mtsMaxAttempt {
			attempt = mtsMaxAttempt
		}
	}
}

func reconnectBackoff(n int) time.Duration {
	if n < 1 {
		n = 1
	}
	shift := n - 1
	if shift > 4 {
		shift = 4
	}
	d := 500 * time.Millisecond * time.Duration(1<<uint(shift))
	if d > 15*time.Second {
		d = 15 * time.Second
	}
	d += time.Duration(mrand.Int63n(int64(d/2) + 1))
	return d
}

func (s *mtsSession) safeWrite(data []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.Conn.SetWriteDeadline(time.Now().Add(mtsWriteDeadline)); err != nil {
		return err
	}
	if err := s.Conn.WriteMessage(websocket.TextMessage, data); err != nil {
		s.Conn.Close()
		return err
	}
	return nil
}

func (s *mtsSession) sendJSON(v map[string]interface{}) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return s.safeWrite(payload)
}

// connectAndServe tries the main pod, then the reserve pod, but only while it is still trying
// to get a session at all. Once a session has been established its end means "reconnect", and
// hopping to the reserve pod instead would leave the two ends on different pods, which looks
// like a live transport that silently carries nothing. It reports whether a session ran.
func (t *Transport) connectAndServe(attempt int, info mtsInfo) (bool, error) {
	endpoints := []struct {
		base  string
		alias string
	}{
		{info.wsDomain, info.pod},
		{info.reserveWs, info.reservePod},
	}

	var lastErr error
	for _, ep := range endpoints {
		if ep.alias == "" {
			utils.Debugf("[MTS] skipping endpoint %s: no pod alias", ep.base)
			continue
		}
		established, err := t.dialAndServe(attempt, info, ep.base, ep.alias)
		if established {
			return true, err
		}
		lastErr = err
		if errors.Is(err, errMTSFatal) {
			return false, err
		}
		select {
		case <-t.done:
			return false, nil
		case <-time.After(time.Second):
		}
	}
	return false, lastErr
}

var errMTSFatal = fmt.Errorf("mts: board rejected the session")

// dialAndServe reports whether a session was actually established before it ended.
func (t *Transport) dialAndServe(attempt int, info mtsInfo, base, alias string) (bool, error) {
	wsURL := fmt.Sprintf("%s/ws/%s?clientUID=%s&locale=ru", base, alias, info.clientUID)

	header := http.Header{}
	header.Set("User-Agent", mtsUA)
	header.Set("Origin", mtsOrigin)
	header.Set("Accept-Language", "ru")

	dialer := websocket.Dialer{
		HandshakeTimeout: mtsDialTimeout,
		NetDialContext:   transport.ProtectedDialer().DialContext,
	}
	utils.Debugf("[MTS] dial %s (attempt %d)", wsURL, attempt)
	if utils.IsVerbose() {
		utils.Debugf("[MTS] dial stack:\n%s", debug.Stack())
	}
	conn, resp, err := dialer.Dial(wsURL, header)
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		return false, fmt.Errorf("dial %s (http %d): %w", wsURL, status, err)
	}
	utils.Debugf("[MTS] WS connected: %s pod=%s", base, alias)

	sess := &mtsSession{Info: info, Conn: conn}
	t.session.Store(sess)
	defer t.session.Store(nil)

	if err := t.handshake(sess); err != nil {
		conn.Close()
		return false, err
	}

	_ = conn.SetReadDeadline(time.Time{})

	t.SetConnected(true)
	t.EmitEvent(transport.EventConnected, strconv.Itoa(attempt+1))

	kaStop := make(chan struct{})
	utils.SafeGo("mts.ping", func() { t.pingLoop(sess, kaStop) })
	defer close(kaStop)

	for {
		select {
		case <-t.done:
			return true, nil
		default:
		}

		_ = conn.SetReadDeadline(time.Now().Add(mtsReadDeadline))
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return true, fmt.Errorf("read: %w", err)
		}
		t.handleMessage(sess, msg)
	}
}

// handshake runs the exact message sequence the board app runs on load: init, deskRequest,
// boardLoaded, boardUsersRequest. The server hands back the sessionUID every fast/view
// message has to be stamped with, so initResponse is mandatory before anything is sent.
func (t *Transport) handshake(sess *mtsSession) error {
	conn := sess.Conn
	_ = conn.SetReadDeadline(time.Now().Add(mtsDialTimeout))

	if err := sess.sendJSON(map[string]interface{}{
		"type":             "init",
		"board_uid":        sess.Info.boardUID,
		"token":            sess.Info.token,
		"boardAccessToken": sess.Info.accessTok,
		"jwt":              "",
		"clientUID":        sess.Info.clientUID,
		"prefix":           sess.Info.prefix,
		"signature":        sess.Info.signature,
		"temporary":        sess.Info.temporary,
		"guestName":        sess.Info.guestName,
	}); err != nil {
		return fmt.Errorf("init: %w", err)
	}

	sessionUID, err := t.awaitSession(conn)
	if err != nil {
		return fmt.Errorf("init response: %w", err)
	}
	sess.sessionUID.Store(&sessionUID)
	utils.Debugf("[MTS] session established: %s", shortStr(sessionUID, 8))

	if err := sess.sendJSON(map[string]interface{}{"type": "deskRequest", "desc_uid": sess.Info.boardUID}); err != nil {
		return fmt.Errorf("deskRequest: %w", err)
	}
	if err := sess.sendJSON(map[string]interface{}{"type": "boardLoaded"}); err != nil {
		return fmt.Errorf("boardLoaded: %w", err)
	}
	if err := sess.sendJSON(map[string]interface{}{"type": "boardUsersRequest"}); err != nil {
		return fmt.Errorf("boardUsersRequest: %w", err)
	}

	_ = conn.SetReadDeadline(time.Time{})
	return nil
}

func (t *Transport) awaitSession(conn *websocket.Conn) (string, error) {
	deadline := time.Now().Add(mtsDialTimeout)
	for time.Now().Before(deadline) {
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return "", err
		}
		var env struct {
			Type       string `json:"type"`
			Status     string `json:"status"`
			SessionUID string `json:"sessionUID"`
		}
		if err := json.Unmarshal(msg, &env); err != nil {
			continue
		}
		switch env.Type {
		case "initResponse":
			if env.Status == "failed" {
				return "", errMTSFatal
			}
			if env.SessionUID == "" {
				return "", fmt.Errorf("initResponse without sessionUID")
			}
			return env.SessionUID, nil
		case "errorResponse":
			if env.Status == "failed" {
				return "", errMTSFatal
			}
		}
	}
	return "", fmt.Errorf("timed out waiting for initResponse")
}

// sendCursor smuggles one already-framed payload through the multi-user cursor channel. This
// is the channel every browser guest uses to broadcast its pointer, and MTS forwards it to
// every other session on the board untouched.
func (t *Transport) sendCursor(sess *mtsSession, frame []byte) error {
	sessionUID := ""
	if p := sess.sessionUID.Load(); p != nil {
		sessionUID = *p
	}
	if sessionUID == "" {
		return fmt.Errorf("no sessionUID")
	}
	return sess.sendJSON(map[string]interface{}{
		"type":    "fast",
		"subtype": "view",
		"data": map[string]interface{}{
			"sessionUID": sessionUID,
			"name":       sess.Info.guestName,
			"login":      "",
			"token":      sess.Info.token,
			"cursorPosition": map[string]interface{}{
				"x": base64.StdEncoding.EncodeToString(frame),
				"y": mtsCursorY,
			},
			"viewPosition": map[string]interface{}{
				"viewportStartX": 0,
				"viewportStartY": 0,
				"viewportWidth":  1280,
				"viewportHeight": 720,
			},
		},
	})
}

func (t *Transport) pingLoop(sess *mtsSession, stop chan struct{}) {
	tick := time.NewTicker(mtsPingInterval)
	defer tick.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.done:
			return
		case <-tick.C:
			if err := sess.sendJSON(map[string]interface{}{"type": "pingRequest"}); err != nil {
				utils.Debugf("[MTS] ping: %v", err)
				return
			}
		}
	}
}

var (
	viewFastMarker = []byte(`"subtype":"view"`)
	viewTypeMarker = []byte(`"type":"fast"`)
)

type mtsEnvelope struct {
	Type       string `json:"type"`
	Subtype    string `json:"subtype"`
	SessionUID string `json:"sessionUID"`
	Data       struct {
		SessionUID     string `json:"sessionUID"`
		CursorPosition struct {
			X json.RawMessage `json:"x"`
			Y float64         `json:"y"`
		} `json:"cursorPosition"`
	} `json:"data"`
}

func (t *Transport) handleMessage(sess *mtsSession, raw []byte) {
	// Every other frame on this socket is board state: a deskRequest answer can be hundreds
	// of KB of JSON. Checking two substrings first keeps that off the JSON parser entirely.
	if !bytes.Contains(raw, viewTypeMarker) || !bytes.Contains(raw, viewFastMarker) {
		return
	}
	var env mtsEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return
	}
	if env.Type != "fast" || env.Subtype != "view" {
		return
	}

	sender := env.Data.SessionUID
	if sender == "" {
		sender = env.SessionUID
	}
	if mine := sess.sessionUID.Load(); mine != nil && sender == *mine {
		return
	}
	if len(env.Data.CursorPosition.X) == 0 {
		return
	}
	var encoded string
	if err := json.Unmarshal(env.Data.CursorPosition.X, &encoded); err != nil || encoded == "" {
		return
	}
	frame, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || len(frame) == 0 {
		return
	}

	t.deliver(frame, sender)
}

func (t *Transport) deliver(frame []byte, sender string) {
	if transport.IsBatchFrame(frame) {
		packets, err := transport.DecodeBatch(frame)
		if err != nil {
			utils.Debugf("[MTS] batch decode error (%d bytes): %v", len(frame), err)
			return
		}
		for _, p := range packets {
			t.emit(p)
		}
		utils.Debugf("[MTS<-] batch from=%s packets=%d bytes=%d", shortStr(sender, 8), len(packets), len(frame))
		return
	}
	// A bare packet from a peer that has not switched to batching yet.
	t.emit(frame)
	utils.Debugf("[MTS<-] packet from=%s bytes=%d", shortStr(sender, 8), len(frame))
}

func (t *Transport) emit(p []byte) {
	if len(p) == 0 {
		return
	}
	start := time.Now()
	t.RecordReceive(len(p))
	t.onDataMu.RLock()
	cb := t.onData
	t.onDataMu.RUnlock()
	if cb != nil {
		cb(p)
	}
	// The read loop is the only thing draining the socket, so a slow consumer here is a
	// stalled tunnel. Worth knowing when a board feels sluggish.
	if utils.IsVerbose() {
		t.slowCallback.Add(int64(time.Since(start)))
	}
}

func shortStr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
