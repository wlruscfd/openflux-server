package yandex

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/http/cookiejar"
	neturl "net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"golang.org/x/crypto/chacha20poly1305"

	"github.com/p1neappleXpress/OpenFlux/transport"
	"github.com/p1neappleXpress/OpenFlux/utils"
)

// writerLoop batches queued packets into one length-prefixed blob (Volga's framing) instead of one WS frame per packet, since per-message overhead dominates at higher packet rates.
const batchMarker = 0xFE

const kaBatchCapabilityToken = "+batch1"

// zstdBatchMarker (0xFE) flags EnableSelfCompression's whole-batch format, distinct from batchMarker and transport.Compress's own 0x00/0x1F first bytes.
const zstdBatchMarker = 0xFD

const kaZstdBatchCapabilityToken = "+zbatch1"

// kaEncSelfCompressToken proves a peer supports EnableEncryptedSelfCompression specifically, since a peer can support zstdBatchMarker while still using the traditional external-wrapper encryption.
const kaEncSelfCompressToken = "+encsc1"

const (
	ydocsBatchSize     = 20
	ydocsBatchTimeout  = 5 * time.Millisecond
	ydocsBatchMaxBytes = 4 * 1024 * 1024
)

// ---------- saveChanges: шаблоны и сборка ----------
//
// В дампе веб-клиента каждое поле фиксировано; вариативны только:
//
//   - UserId / UserShortId  — id участника (из editor_config.user.id);
//   - cursorOps             — позиции/состояние каретки (по одному на сдвиг);
//   - appVersion            — версия клиента (в оп-метаданных, зашита в base64).
//
// Формат самих оп — закрытый бинарник редактора. Мы не пытаемся его
// изобрести заново: воспроизводим ровно тот же префикс-entity и
// base64-полезную нагрузку, что и оригинал, а меняем лишь то, что реально
// можно менять, не порождая «мусорных» операций.
//
// modelMetaOp — служебный оп модели: заголовок сессии + версия клиента.
// Он не зависит от пользователя, но привязан к версии editor’а. Менять его
// содержимое нельзя без смены формата; вынесен отдельной константой, чтобы
// при апдейте редактора его можно было заменить одной строкой.
const (
	modelMetaOp  = "76;AgAAADEA//8BAJ+fl7jkAwIALQEAAAMAAAAAAAAAAAAAAAAAAAAAAAAA9v///xoAAAAyADAAMgA2AC4AMgAuADEALgAyADIANgA4AA=="
	entityCursor = "35" // id потока курсора (совпадает с оригиналом)
	cursorStream = "14" // id курсор-потока в CursorInfo
)

// cursorOp собирает один курсор-оп: "<entity>;<base64>". Base64-нагрузка —
// та же форма, что в дампе (4-байтовый заголовок длины строки, затем строка
// userShortID в UTF-16LE, затем фиксированные поля и байт seq), но с
// настоящим userShortID и меняющимся счётчиком seq, поэтому каждое
// сообщение отличается и выглядит живой активностью.
func cursorOp(userShortID string, seq int) string {
	payload := make([]byte, 0, 32)
	payload = append(payload, 0x06, 0x00, 0x00, 0x00) // длина строки = 6 (UTF-16 units)
	for _, r := range userShortID {
		payload = append(payload, byte(r), 0x00)
	}
	payload = append(payload,
		0x01, 0x00, 0x1c, 0x00, // позиция/флаг курсора
		0x01, 0x00, 0x00, 0x00,
		byte(seq), 0x00, 0x00, 0x00,
		0x01, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
	)
	return entityCursor + ";" + base64.StdEncoding.EncodeToString(payload)
}

// cursorInfo строит CursorInfo-строку: "<stream>;<base64>". В base64 лежит
// 6-байтовая строка userShortID (UTF-16LE) и 4 байта позиции; формат ровно
// такой же, как в дампе, но id и позиция — настоящие.
func cursorInfo(userShortID string, seq int) string {
	payload := make([]byte, 0, 16)
	payload = append(payload, 0x06, 0x00, 0x00, 0x00)
	for _, r := range userShortID {
		payload = append(payload, byte(r), 0x00)
	}
	payload = append(payload, 0x03, 0x00, 0x00, 0x00, byte(seq), 0x00)
	return cursorStream + ";" + base64.StdEncoding.EncodeToString(payload)
}

// BuildSaveChanges is the saveChanges message for one participant and
// iteration (see buildSaveChanges for how the pieces are chosen). A pure
// function of its arguments, exported so the JS port of this transport can be
// compared with it byte for byte.
func BuildSaveChanges(userID string, isExcel bool, seq int) []byte {
	short := userID
	if len(short) > 10 {
		short = short[:10]
	}

	changes := []string{
		modelMetaOp,
		cursorOp(short, seq),
		cursorOp(short, seq+1),
		cursorOp(short, seq+2),
	}
	changesJSON, _ := json.Marshal(changes) // ["...","..."]

	excelInfo := map[string]string{
		"UserId":      userID,
		"UserShortId": short,
		"CursorInfo":  cursorInfo(short, seq),
	}
	excelJSON, _ := json.Marshal(excelInfo)

	msg := map[string]interface{}{
		"type":                "saveChanges",
		"changes":             string(changesJSON),
		"startSaveChanges":    true,
		"endSaveChanges":      true,
		"isCoAuthoring":       true,
		"isExcel":             isExcel,
		"deleteIndex":         nil,
		"excelAdditionalInfo": string(excelJSON),
		"unlock":              false,
		"releaseLocks":        true,
	}
	body, _ := json.Marshal([]interface{}{"message", msg})
	return append([]byte("42"), body...)
}

// buildSaveChanges собирает saveChanges-сообщение под конкретную сессию и
// номер итерации: актуальные UserId/UserShortId и слегка меняющиеся позиции
// курсора. Оригинальные entity-префиксы и модель-оп сохранены побайтово;
// меняется только то, что действительно зависит от участника.
func (t *YandexDocsTransport) buildSaveChanges(session *DocSession, seq int) []byte {
	userID := session.Info.EditorUserID
	if userID == "" {
		userID = session.UserID // fallback: псевдо-id, если jwt не дал реального
	}
	return BuildSaveChanges(userID, session.Info.IsExcel, seq)
}
// wsWriteTimeout bounds every WebSocket write; without it a stalled write blocks writerLoop, the one goroutine draining a session's queue, forever.
const wsWriteTimeout = 10 * time.Second

const defaultPingWindow = 45 * time.Second

const handshakeTimeout = 15 * time.Second

const (
	reasonFetchFailed     = "fetch_failed"
	reasonDialFailed      = "dial_failed"
	reasonHandshakeFailed = "handshake_failed"
	reasonSendFailed      = "send_failed"
	reasonReadError       = "read_error"
	reasonCaptchaBlocked  = "captcha_blocked"
)

// errCaptchaBlocked marks a CAPTCHA/bot-check page returned instead of the doc editor.
var errCaptchaBlocked = errors.New("captcha or bot-check page returned instead of the doc editor")

// captchaCooldown is a floor under the usual attempt-scaled backoff for captcha failures.
const captchaCooldown = 3 * time.Minute

// cookieRetryFloor applies when a fresh jar arrives mid-fetch: worth retrying promptly, but
// never with a zero wait, or a client re-pushing the same unsolved jar spins the transport.
const cookieRetryFloor = 5 * time.Second

type YandexDocsInfo struct {
	CookieStr    string
	Token        string
	DocID        string
	CallbackURL  string
	UserID       string
	EditorUserID string // editor_config.user.id: настоящий id участника в документе
	IsExcel      bool   // editor_config.document.fileType ∈ {xlsx,xls,xlsm,csv}
	Origin       string
	Host         string
	WsURL        string
	Permissions  map[string]interface{}
	OpenCmd      map[string]interface{}
}

type DocSession struct {
	Info       YandexDocsInfo
	Conn       *websocket.Conn
	WriteQueue chan []byte
	UserID     string
	writeMu    sync.Mutex
}

func (s *DocSession) safeWrite(messageType int, data []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.Conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
	err := s.Conn.WriteMessage(messageType, data)
	if err != nil {
		s.Conn.Close() // let the read loop notice and reconnect
	}
	return err
}

type YandexDocsTransport struct {
	*transport.BaseTransport

	url     string
	session *DocSession

	userCounter atomic.Int32
	baseUserID  string

	// recentSent guards against self-echo: Yandex's doc broadcasts every event to every participant including the sender, so a short-lived hash of sent payloads lets a matching inbound be dropped.
	recentSentMu sync.Mutex
	recentSent   map[uint32]time.Time

	peerBatches atomic.Bool

	// peerZstdBatches is peerBatches' counterpart for zstdBatchMarker.
	peerZstdBatches atomic.Bool

	// selfCompress/encrypted/encSend/encRecv are set once before Start and never written again, so reading them without a lock from writerLoop/handleMessage is safe.
	selfCompress bool

	encrypted  bool
	encSend    [chacha20poly1305.KeySize]byte
	encRecv    [chacha20poly1305.KeySize]byte
	encSendCtr atomic.Uint64

	peerEncSelfCompress atomic.Bool

	wakeReconnect chan struct{}

	// tag identifies this instance's log lines on a node running many keys at once - a bare "[YDOCS]" line can't otherwise be traced back to which key it belongs to.
	tag string

	// providedCookies is set via ProvideCookies and resent on every future fetch.
	providedCookies         string
	cookieCutPending        bool
	captchaCookieGeneration atomic.Uint64

	captchaSolveMode CaptchaSolveMode
}

// CaptchaSolveMode selects how an exit node reacts to a CAPTCHA failure - exit-node use only.
type CaptchaSolveMode string

const (
	CaptchaSolveModeOff      CaptchaSolveMode = ""
	CaptchaSolveModeNative   CaptchaSolveMode = "native"
	CaptchaSolveModeExternal CaptchaSolveMode = "external"
)

func (t *YandexDocsTransport) SetCaptchaSolveMode(mode CaptchaSolveMode) {
	t.captchaSolveMode = mode
}

func (t *YandexDocsTransport) debugf(format string, args ...interface{}) {
	utils.Debugf("[YDOCS/%s] "+format, append([]interface{}{t.tag}, args...)...)
}

// ProvideCookies feeds a solved session's cookies into subsequent fetches and forces a reconnect.
func (t *YandexDocsTransport) ProvideCookies(cookieStr string) {
	normalized := normalizeCookieHeader(cookieStr)

	t.Mu.Lock()
	if normalized == t.providedCookies {
		t.Mu.Unlock()
		t.debugf("ignoring repeated push of the same %d cookie bytes", len(cookieStr))
		return
	}
	t.providedCookies = normalized
	t.Mu.Unlock()

	sum := sha256.Sum256([]byte(normalized))
	t.debugf("pushed cookie jar: %d bytes, sha256 %x", len(normalized), sum[:6])

	t.captchaCookieGeneration.Add(1)
	// The client re-solves on every poll and its jar churns between attempts, so cutting the
	// wait short every time turns one solved captcha into a hot loop. One push may cut the wait
	// per retry episode; the rest are stored and picked up by the next scheduled attempt.
	t.Mu.Lock()
	cut := !t.cookieCutPending
	if cut {
		t.cookieCutPending = true
	}
	t.Mu.Unlock()

	if !cut {
		t.debugf("new %d byte cookie jar, applying it on the next retry window", len(cookieStr))
		return
	}
	t.debugf("received %d bytes of externally-solved cookies, forcing a reconnect", len(cookieStr))
	t.ForceReconnect()
}

// cookieNames lists only the names of the cookies a jar would send for a URL: enough to tell
// "the jar arrived" from "the jar arrived but is missing the session cookie" without logging
// secret values.
func cookieNames(cookies []*http.Cookie) string {
	if len(cookies) == 0 {
		return "(none)"
	}
	names := make([]string, 0, len(cookies))
	for _, c := range cookies {
		names = append(names, c.Name)
	}
	return strings.Join(names, ",")
}

func shortURL(u string) string {
	if len(u) > 90 {
		return u[:90] + "..."
	}
	return u
}

func pageTitle(body []byte) string {
	m := regexp.MustCompile(`(?is)<title[^>]*>(.*?)</title>`).FindSubmatch(body)
	if m == nil {
		return ""
	}
	return strings.TrimSpace(string(m[1]))
}

// normalizeCookieHeader sorts the pairs so the same jar in a different order compares equal.
// The WebView rebuilds the header from a map, so its order changes on every poll and a plain
// string comparison would treat a re-push of the identical jar as new credentials.
func normalizeCookieHeader(header string) string {
	parts := strings.Split(header, ";")
	cleaned := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			cleaned = append(cleaned, p)
		}
	}
	sort.Strings(cleaned)
	return strings.Join(cleaned, "; ")
}

// solveCaptchaIsolated keeps the proof-of-work solver off the live jar. The solver's own
// Set-Cookie responses otherwise overwrite an externally-solved session, so a good jar
// stops working after the first captcha attempt and the client re-pushes it forever.
func solveCaptchaIsolated(docURL string, jar http.CookieJar, userAgent string, rt http.RoundTripper) error {
	temp, err := cookiejar.New(nil)
	if err != nil {
		return err
	}
	parsed, err := neturl.Parse(docURL)
	if err != nil {
		return err
	}
	temp.SetCookies(parsed, jar.Cookies(parsed))
	acceptedURL, err := solveCaptcha(docURL, temp, userAgent, rt)
	if err != nil {
		return err
	}
	jar.SetCookies(parsed, temp.Cookies(parsed))
	if accepted, perr := neturl.Parse(acceptedURL); perr == nil && accepted.Host != parsed.Host {
		jar.SetCookies(accepted, temp.Cookies(accepted))
	}
	return nil
}

func (t *YandexDocsTransport) getProvidedCookies() string {
	t.Mu.RLock()
	defer t.Mu.RUnlock()
	return t.providedCookies
}

func parseCookieHeader(header string) []*http.Cookie {
	req := &http.Request{Header: http.Header{"Cookie": {header}}}
	return req.Cookies()
}

func mergeCookieHeaders(fresh, provided string) string {
	values := make(map[string]string)
	order := make([]string, 0)
	add := func(header string, override bool) {
		for _, c := range parseCookieHeader(header) {
			if _, ok := values[c.Name]; !ok {
				order = append(order, c.Name)
			}
			if override || values[c.Name] == "" {
				values[c.Name] = c.Value
			}
		}
	}
	add(fresh, false)
	add(provided, true)
	parts := make([]string, 0, len(order))
	for _, name := range order {
		parts = append(parts, name+"="+values[name])
	}
	return strings.Join(parts, "; ")
}

// EnableSelfCompression makes writerLoop zstd-compress a whole batch of raw packets once the peer's keepalive proves it understands zstdBatchMarker, beating per-packet LZ4's missed cross-packet redundancy; not for use alongside external CompressedTransport wrapping.
func (t *YandexDocsTransport) EnableSelfCompression() {
	t.selfCompress = true
}

// EnableEncryptedSelfCompression batches and zstd-compresses raw packets first, then encrypts the whole compressed batch as one unit, since encrypting per-packet first would destroy the cross-packet redundancy batching exploits; gated by its own kaEncSelfCompressToken separate from zstdBatchMarker support.
func (t *YandexDocsTransport) EnableEncryptedSelfCompression(token string, isExitNode bool) {
	t.enableEncryptedSelfCompression(token, isExitNode, "")
}

func (t *YandexDocsTransport) EnableEncryptedSelfCompressionForStream(token string, isExitNode bool, streamIndex int) {
	t.enableEncryptedSelfCompression(token, isExitNode, fmt.Sprintf(" stream %d", streamIndex))
}

func (t *YandexDocsTransport) enableEncryptedSelfCompression(token string, isExitNode bool, infoSuffix string) {
	t.selfCompress = true
	t.encrypted = true
	t.encSend, t.encRecv = transport.DeriveDirectionalKeys(token, isExitNode, infoSuffix)
}

func NewYandexDocsTransport(url string, config transport.TransportConfig) *YandexDocsTransport {
	normalized := normalizeDocURL(url)
	t := &YandexDocsTransport{
		BaseTransport: transport.NewBaseTransport(config),
		url:           normalized,
		tag:           fmt.Sprintf("%06x", crc32.ChecksumIEEE([]byte(normalized))),
	}
	t.baseUserID = randUserID()
	return t
}

func normalizeDocURL(raw string) string {
	u, err := neturl.Parse(raw)
	if err != nil {
		return raw
	}
	if strings.EqualFold(u.Hostname(), "disk.yandex.ru") && strings.HasPrefix(u.Path, "/i/") {
		u.Host = "docs.yandex.ru"
		return u.String()
	}
	return raw
}

func (t *YandexDocsTransport) Start() error {
	if err := t.BaseTransport.Start(); err != nil {
		return err
	}

	t.baseUserID = randUserID()
	go t.keepAliveLoop()
	t.connectToDoc(0)

	return nil
}

// Stop closes the live session's socket, not just BaseTransport's flag: an in-flight ReadMessage()
// blocks up to the ping window (tens of seconds) otherwise, leaking a goroutine and a socket per
// stop under the churn a busy control plane produces (e.g. a quota-triggered worker restart).
func (t *YandexDocsTransport) Stop() error {
	t.BaseTransport.Stop()
	t.Mu.Lock()
	session := t.session
	t.Mu.Unlock()
	if session != nil && session.Conn != nil {
		session.Conn.Close()
	}
	return nil
}

// Send doesn't require IsConnected(): a session's WriteQueue is reused across a reconnect so a brief drop can queue data instead of forcing the tunnel's own TCP to notice loss and retransmit.
func (t *YandexDocsTransport) Send(data []byte) error {
	t.Mu.RLock()
	session := t.session
	t.Mu.RUnlock()

	if session == nil {
		return fmt.Errorf("no active session")
	}

	packet := make([]byte, len(data))
	copy(packet, data)
	select {
	case session.WriteQueue <- packet:
		t.RecordSend(len(data))
		return nil
	default:
		// Previously silent: the caller (tunnelEP's outgoing handler) drops this error on the floor, so a drop here was invisible in every log.
		t.debugf("write queue full, dropping %d bytes", len(data))
		return fmt.Errorf("write queue full")
	}
}

func (t *YandexDocsTransport) connectToDoc(attempt int) {
	if !t.IsRunning() {
		return
	}

	t.debugf("connectToDoc attempt %d", attempt)
	t.EmitEvent(transport.EventConnecting, strconv.Itoa(attempt+1))

	go func() {
		t.Mu.Lock()
		existingSession := t.session
		t.Mu.Unlock()

		var userID string
		if existingSession != nil {
			userID = existingSession.UserID
		} else {
			suffix := fmt.Sprintf("%03d", t.userCounter.Add(1)%1000)
			userID = t.baseUserID + suffix
		}

		cookieGeneration := t.captchaCookieGeneration.Load()
		info, err := t.fetchDocInfo(t.url, userID)
		if err != nil {
			t.debugf("fetchDocInfo failed: %v", err)
			if errors.Is(err, errCaptchaBlocked) {
				if t.captchaCookieGeneration.Load() != cookieGeneration {
					t.debugf("captcha cookies changed during fetch, retrying soon")
					t.scheduleReconnectWithMinDelay(attempt, reasonCaptchaBlocked, err, cookieRetryFloor)
					return
				}
				t.scheduleReconnectWithMinDelay(attempt, reasonCaptchaBlocked, err, captchaCooldown)
			} else {
				t.scheduleReconnect(attempt, reasonFetchFailed, err)
			}
			return
		}

		dialer := websocket.Dialer{
			HandshakeTimeout:  10 * time.Second,
			EnableCompression: true, // negotiated (permessage-deflate); harmless if the server ignores it
			NetDialContext:    transport.ProtectedDialer().DialContext,
		}
		headers := http.Header{}
		headers.Set("User-Agent", browserUserAgent)
		headers.Set("Origin", info.Origin)
		headers.Set("Cookie", info.CookieStr)
		headers.Set("Host", info.Host)
		headers.Set("Sec-Fetch-Dest", "websocket")
		headers.Set("Sec-Fetch-Mode", "websocket")
		headers.Set("Sec-Fetch-Site", "same-origin")

		conn, _, err := dialer.Dial(info.WsURL, headers)
		if err != nil {
			t.debugf("WebSocket dial failed: %v", err)
			t.scheduleReconnect(attempt, reasonDialFailed, err)
			return
		}

		// The engine.io/socket.io handshake must complete before anything else goes over this socket, or OnlyOffice's backend tears the connection down with close code 1005.
		readTimeout, err := t.performHandshake(conn, info.Token)
		if err != nil {
			t.debugf("handshake failed: %v", err)
			conn.Close()
			t.scheduleReconnect(attempt, reasonHandshakeFailed, err)
			return
		}

		if !t.IsRunning() {
			// Stop() ran while this goroutine was still dialing/handshaking - closing here (rather
			// than going live) is the only thing that would have closed this particular conn.
			conn.Close()
			return
		}

		writeQueue := make(chan []byte, t.GetConfig().MaxQueueSize)
		if existingSession != nil {
			writeQueue = existingSession.WriteQueue
		}

		session := &DocSession{
			Info:       info,
			Conn:       conn,
			WriteQueue: writeQueue,
			UserID:     userID,
		}

		t.Mu.Lock()
		t.session = session
		t.Mu.Unlock()

		if existingSession == nil {
			go t.writerLoop(writeQueue)
		}

		authData := map[string]interface{}{
			"type": "auth", "docid": info.DocID, "token": "fghhfgsjdgfjs",
			"user": map[string]interface{}{"id": userID}, "editorType": 0,
			"lastOtherSaveTime": -1, "permissions": info.Permissions,
			"openCmd": info.OpenCmd, "coEditingMode": "fast", "jwtOpen": info.Token,
		}
		messagePart, err := json.Marshal([]interface{}{"message", authData})
		if err != nil {
			t.debugf("marshal auth message failed: %v", err)
			conn.Close()
			t.scheduleReconnect(attempt, reasonSendFailed, err)
			return
		}
		if err := session.safeWrite(websocket.TextMessage, []byte(fmt.Sprintf("42%s", string(messagePart)))); err != nil {
			t.debugf("send auth message failed: %v", err)
			conn.Close()
			t.scheduleReconnect(attempt, reasonSendFailed, err)
			return
		}
		// Must come after auth, not before - a queued writerLoop backlog could otherwise beat it onto the wire.
		t.SetConnected(true)
		t.EmitEvent(transport.EventConnected, strconv.Itoa(attempt+1))
		connectedAt := time.Now()

		for t.IsRunning() {
			conn.SetReadDeadline(time.Now().Add(readTimeout))
			_, message, err := conn.ReadMessage()
			if err != nil {
				t.debugf("Read error: %v", err)
				t.SetConnected(false)
				conn.Close()
				// A session that stayed up a while before dropping counts as a normal blip, not evidence backoff should keep growing, or a long-lived transport's backoff ratchets up and stays maxed forever.
				next := attempt
				if time.Since(connectedAt) > 15*time.Second {
					next = 0
				}
				t.scheduleReconnect(next, reasonReadError, err)
				return
			}
			t.handleMessage(session, message)
		}
	}()
}

// performHandshake waits out the engine.io open packet, then the socket.io namespace-connect ack (the socket is usable only after); pings are answered immediately regardless of handshake progress.
func (t *YandexDocsTransport) performHandshake(conn *websocket.Conn, token string) (time.Duration, error) {
	conn.SetReadDeadline(time.Now().Add(handshakeTimeout))
	defer conn.SetReadDeadline(time.Time{})

	readTimeout := defaultPingWindow
	sentConnect := false

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return 0, fmt.Errorf("handshake read: %w", err)
		}
		text := string(msg)

		switch {
		case !sentConnect && strings.HasPrefix(text, "0"):
			var openPkt struct {
				PingInterval int `json:"pingInterval"`
				PingTimeout  int `json:"pingTimeout"`
			}
			if err := json.Unmarshal([]byte(text[1:]), &openPkt); err == nil &&
				openPkt.PingInterval > 0 && openPkt.PingTimeout > 0 {
				readTimeout = time.Duration(openPkt.PingInterval+openPkt.PingTimeout) * time.Millisecond
			}

			authPkt, err := json.Marshal(map[string]string{"token": token})
			if err != nil {
				return 0, fmt.Errorf("marshal namespace-connect: %w", err)
			}
			conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
			if err := conn.WriteMessage(websocket.TextMessage, append([]byte("40"), authPkt...)); err != nil {
				return 0, fmt.Errorf("send namespace-connect: %w", err)
			}
			sentConnect = true

		case text == "2":
			conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
			if err := conn.WriteMessage(websocket.TextMessage, []byte("3")); err != nil {
				return 0, fmt.Errorf("pong during handshake: %w", err)
			}

		case strings.HasPrefix(text, "44"):
			return 0, fmt.Errorf("namespace connect rejected: %s", text)

		case sentConnect && strings.HasPrefix(text, "40"):
			return readTimeout, nil

		default:
			t.debugf("unexpected message during handshake: %s", text)
		}
	}
}

func (t *YandexDocsTransport) writerLoop(queue chan []byte) {
	batch := make([][]byte, 0, ydocsBatchSize)
	totalBytes := 0

	flush := func(session *DocSession) {
		if len(batch) == 0 {
			return
		}
		switch {
		case t.encrypted && t.peerEncSelfCompress.Load():
			t.sendEncryptedZstdBatch(session, batch)
		case t.encrypted:
			// Peer hasn't proven encrypted self-compression for this key, so reproduce the traditional compress-then-encrypt-each-packet format for compatibility.
			sealed := make([][]byte, 0, len(batch))
			for _, pkt := range batch {
				ciphertext, err := transport.Seal(t.encSend, t.encSendCtr.Add(1), transport.Compress(pkt))
				if err != nil {
					t.debugf("encrypt failed, dropping packet: %v", err)
					continue
				}
				sealed = append(sealed, ciphertext)
			}
			if len(sealed) > 0 {
				if t.peerBatches.Load() {
					t.sendBatch(session, sealed)
				} else {
					for _, pkt := range sealed {
						t.sendSingle(session, pkt)
					}
				}
			}
		case t.selfCompress && t.peerZstdBatches.Load():
			t.sendZstdBatch(session, batch)
		case t.selfCompress:
			// Peer hasn't proven zstdBatchMarker support, so reproduce transport.CompressedTransport's per-packet format so the wire bytes match what it expects.
			compressed := make([][]byte, len(batch))
			for i, pkt := range batch {
				compressed[i] = transport.Compress(pkt)
			}
			if t.peerBatches.Load() {
				t.sendBatch(session, compressed)
			} else {
				for _, pkt := range compressed {
					t.sendSingle(session, pkt)
				}
			}
		case t.peerBatches.Load():
			t.sendBatch(session, batch)
		default:
			for _, pkt := range batch {
				t.sendSingle(session, pkt)
			}
		}
		batch = batch[:0]
		totalBytes = 0
	}

	for t.IsRunning() {
		// t.session is never nil'd on disconnect, so a nil check alone never catches a drop - IsConnected() is what actually keeps queued data queued until a live session exists.
		t.Mu.RLock()
		session := t.session
		connected := t.IsConnected()
		t.Mu.RUnlock()
		if session == nil || session.Conn == nil || !connected {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		select {
		case packet := <-queue:
			batch = append(batch, packet)
			totalBytes += len(packet)
			if len(batch) >= ydocsBatchSize || totalBytes >= ydocsBatchMaxBytes {
				flush(session)
			}
		case <-time.After(ydocsBatchTimeout):
			flush(session)
		}
	}
}

func (t *YandexDocsTransport) sendBatch(session *DocSession, batch [][]byte) {
	var blob bytes.Buffer
	blob.WriteByte(batchMarker)
	var lenBuf [2]byte
	for _, p := range batch {
		binary.BigEndian.PutUint16(lenBuf[:], uint16(len(p)))
		blob.Write(lenBuf[:])
		blob.Write(p)
	}
	framed := blob.Bytes()

	t.markSent(framed)
	if utils.IsVerbose() {
		t.debugf("-> %d bytes (%d packets)\n", len(framed), len(batch))
	}

	payload := base64.StdEncoding.EncodeToString(framed)
	msg := fmt.Sprintf(`42["message",{"type":"cursor","cursor":"18;%s"}]`, payload)

	if err := session.safeWrite(websocket.TextMessage, []byte(msg)); err != nil {
		t.debugf("Write error: %v", err)
	}
}

func (t *YandexDocsTransport) sendZstdBatch(session *DocSession, batch [][]byte) {
	encoded := transport.EncodeBatch(batch)
	framed := make([]byte, 1+len(encoded))
	framed[0] = zstdBatchMarker
	copy(framed[1:], encoded)

	t.markSent(framed)
	if utils.IsVerbose() {
		t.debugf("-> %d bytes (%d packets, zstd batch)\n", len(framed), len(batch))
	}

	payload := base64.StdEncoding.EncodeToString(framed)
	msg := fmt.Sprintf(`42["message",{"type":"cursor","cursor":"18;%s"}]`, payload)

	if err := session.safeWrite(websocket.TextMessage, []byte(msg)); err != nil {
		t.debugf("Write error: %v", err)
	}
}

func (t *YandexDocsTransport) sendEncryptedZstdBatch(session *DocSession, batch [][]byte) {
	encoded := transport.EncodeBatch(batch)
	plaintext := make([]byte, 1+len(encoded))
	plaintext[0] = zstdBatchMarker
	copy(plaintext[1:], encoded)

	ciphertext, err := transport.Seal(t.encSend, t.encSendCtr.Add(1), plaintext)
	if err != nil {
		t.debugf("encrypt failed, dropping batch: %v", err)
		return
	}

	t.markSent(ciphertext)
	if utils.IsVerbose() {
		t.debugf("-> %d bytes (%d packets, encrypted zstd batch)\n", len(ciphertext), len(batch))
	}

	payload := base64.StdEncoding.EncodeToString(ciphertext)
	msg := fmt.Sprintf(`42["message",{"type":"cursor","cursor":"18;%s"}]`, payload)

	if err := session.safeWrite(websocket.TextMessage, []byte(msg)); err != nil {
		t.debugf("Write error: %v", err)
	}
}

func (t *YandexDocsTransport) sendSingle(session *DocSession, packet []byte) {
	t.markSent(packet)
	if utils.IsVerbose() {
		t.debugf("-> %d bytes (unbatched)\n", len(packet))
	}

	payload := base64.StdEncoding.EncodeToString(packet)
	msg := fmt.Sprintf(`42["message",{"type":"cursor","cursor":"18;%s"}]`, payload)

	if err := session.safeWrite(websocket.TextMessage, []byte(msg)); err != nil {
		t.debugf("Write error: %v", err)
	}
}

func (t *YandexDocsTransport) markSent(data []byte) {
	h := crc32.ChecksumIEEE(data)
	now := time.Now()

	t.recentSentMu.Lock()
	defer t.recentSentMu.Unlock()
	if t.recentSent == nil {
		t.recentSent = make(map[uint32]time.Time)
	}
	t.recentSent[h] = now
	if len(t.recentSent) > 512 {
		cutoff := now.Add(-5 * time.Second)
		for k, ts := range t.recentSent {
			if ts.Before(cutoff) {
				delete(t.recentSent, k)
			}
		}
	}
}

func (t *YandexDocsTransport) wasRecentlySent(data []byte) bool {
	h := crc32.ChecksumIEEE(data)

	t.recentSentMu.Lock()
	ts, ok := t.recentSent[h]
	t.recentSentMu.Unlock()

	return ok && time.Since(ts) < 5*time.Second
}

func (t *YandexDocsTransport) keepAliveLoop() {
	ticker := time.NewTicker(t.GetConfig().KeepAliveInterval)
	defer ticker.Stop()
	// kaEncSelfCompressToken is conditional on t.encrypted (must prove this instance is in encrypted mode); the other two capability tokens are sent unconditionally.
	keepAliveMsg := `42["message",{"type":"cursor","cursor":"18;---KA---` + kaBatchCapabilityToken + kaZstdBatchCapabilityToken
	if t.encrypted {
		keepAliveMsg += kaEncSelfCompressToken
	}
	keepAliveMsg += `"}]`

	for t.IsRunning() {
		<-ticker.C
		t.Mu.Lock()
		session := t.session
		t.Mu.Unlock()

		// IsConnected() too, not just session/Conn - avoids the same pre-auth send window writerLoop had.
		if session != nil && session.Conn != nil && t.IsConnected() {
			if err := session.safeWrite(websocket.TextMessage, []byte(keepAliveMsg)); err != nil {
				t.debugf("Keep-alive failed: %v", err)
				t.SetConnected(false)
				session.Conn.Close()
			}
		}
	}
}

// editorActivityLoop периодически отправляет в документ saveChanges-сообщение,
// собранное под текущую сессию (см. buildSaveChanges): актуальные
// UserId/UserShortId и слегка меняющиеся позиции курсора. Задержка между
// отправками — случайная в диапазоне [0.5 с, 5 с], чтобы поток выглядел как
// правки живого человека: ровный по сути, но не метрономом. Сообщение уходит
// через ту же сессию, что writerLoop и keepAliveLoop, и так же терпит
// переподключение — если сессии сейчас нет, итерация просто пропускается.
func (t *YandexDocsTransport) editorActivityLoop() {
	const (
		minDelay = 500 * time.Millisecond
		maxDelay = 5 * time.Second
	)
	seq := 0
	for t.IsRunning() {
		span := int64(maxDelay - minDelay)
		delay := minDelay + time.Duration(rand.Int63n(span+1))

		select {
		case <-time.After(delay):
		case <-t.Done():
			return
		}

		t.Mu.RLock()
		session := t.session
		t.Mu.RUnlock()
		if session == nil || session.Conn == nil {
			continue // mid-reconnect: nothing to write into
		}

		seq++
		msg := t.buildSaveChanges(session, seq)
		if err := session.safeWrite(websocket.TextMessage, msg); err != nil {
			utils.Debugf("[YDOCS] saveChanges write error: %v", err)
			// Держим цикл: следующая итерация попробует снова, а
			// переподключение (keepAliveLoop / reader) поднимет новый conn.
		}
	}
}

func (t *YandexDocsTransport) handleMessage(session *DocSession, data []byte) {
	text := string(data)

	if strings.Contains(text, "---KA---") {
		if strings.Contains(text, kaBatchCapabilityToken) {
			t.peerBatches.Store(true)
		}
		if strings.Contains(text, kaZstdBatchCapabilityToken) {
			t.peerZstdBatches.Store(true)
		}
		if strings.Contains(text, kaEncSelfCompressToken) {
			t.peerEncSelfCompress.Store(true)
		}
		return
	}

	if text == "2" {
		if session != nil && session.Conn != nil {
			session.safeWrite(websocket.TextMessage, []byte("3"))
		}
		return
	}
	if text == "3" {
		return
	}

	if strings.Contains(text, "saveChanges") || strings.Contains(text, "cursor") {
		base64Str := t.extractBase64String(text)
		if base64Str == "" {
			return
		}

		decoded, err := base64.StdEncoding.DecodeString(base64Str)
		if err != nil {
			t.debugf("Base64 decode error: %v", err)
			return
		}

		if t.wasRecentlySent(decoded) {
			if utils.IsVerbose() {
				t.debugf("dropped self-echo (%d bytes)\n", len(decoded))
			}
			return
		}

		if utils.IsVerbose() {
			t.debugf("<- %d bytes\n", len(decoded))
		}

		t.RecordReceive(len(decoded))

		if t.encrypted {
			t.handleEncryptedMessage(decoded)
			return
		}

		if len(decoded) > 0 && decoded[0] == zstdBatchMarker {
			pkts, err := transport.DecodeBatch(decoded[1:])
			if err != nil {
				t.debugf("zstd batch decode error: %v", err)
				return
			}
			for _, pkt := range pkts {
				t.CallReceive(pkt)
			}
			return
		}

		// In self-compress mode there's no external CompressedTransport to decompress a batchMarker-wrapped batch, so this decompresses it before handing packets upward.
		if len(decoded) > 0 && decoded[0] == batchMarker {
			for _, pkt := range decodeBatch(decoded[1:]) {
				if !t.selfCompress {
					t.CallReceive(pkt)
					continue
				}
				raw, err := transport.Decompress(pkt)
				if err != nil {
					t.debugf("batch item decompress error: %v", err)
					continue
				}
				t.CallReceive(raw)
			}
			return
		}

		if !t.selfCompress {
			t.CallReceive(decoded)
			return
		}
		raw, err := transport.Decompress(decoded)
		if err != nil {
			t.debugf("decompress error: %v", err)
			return
		}
		t.CallReceive(raw)
	}
}

// handleEncryptedMessage dispatches by wire format: the legacy fallback has an unencrypted batchMarker wrapping individually-encrypted items, while the other formats are entirely ciphertext, distinguishable only after decrypting.
func (t *YandexDocsTransport) handleEncryptedMessage(decoded []byte) {
	if len(decoded) > 0 && decoded[0] == batchMarker {
		for _, item := range decodeBatch(decoded[1:]) {
			plain, err := transport.Open(t.encRecv, item)
			if err != nil {
				t.debugf("batch item decrypt failed: %v", err)
				continue
			}
			raw, err := transport.Decompress(plain)
			if err != nil {
				t.debugf("batch item decompress error: %v", err)
				continue
			}
			t.CallReceive(raw)
		}
		return
	}

	plaintext, err := transport.Open(t.encRecv, decoded)
	if err != nil {
		t.debugf("decrypt failed - dropping message: %v", err)
		return
	}

	if len(plaintext) > 0 && plaintext[0] == zstdBatchMarker {
		pkts, err := transport.DecodeBatch(plaintext[1:])
		if err != nil {
			t.debugf("encrypted zstd batch decode error: %v", err)
			return
		}
		for _, pkt := range pkts {
			t.CallReceive(pkt)
		}
		return
	}

	raw, err := transport.Decompress(plaintext)
	if err != nil {
		t.debugf("decompress error: %v", err)
		return
	}
	t.CallReceive(raw)
}

func (t *YandexDocsTransport) extractBase64String(response string) string {
	return ExtractBase64(response)
}

// ExtractBase64 pulls the packet payload out of a server frame: from a
// saveChanges message's excelAdditionalInfo, otherwise from a cursor field.
// Pure, and exported so the JS port can be compared with it.
func ExtractBase64(response string) string {
	if strings.Contains(response, "saveChanges") {
		marker := `"excelAdditionalInfo":"`
		left := strings.Index(response, marker) + len(marker)
		if left < len(marker) {
			return ""
		}
		right := strings.Index(response[left:], `"`)
		if right == -1 {
			return ""
		}
		return response[left : left+right]
	}

	// Anchored on our own literal "18;" marker (see sendBatch/sendSingle/etc.), not just any cursor value with a semicolon - a real participant's own cursor broadcast (this is a real collaborative doc, other Yandex users' cursors go out the same "cursor" field) matched the old, looser pattern too, handing their cursor position to CallReceive as if it were tunnel data.
	re := regexp.MustCompile(`"cursor":"18;([^"]+)"`)
	matches := re.FindStringSubmatch(response)
	if len(matches) > 1 {
		return matches[1]
	}
	return ""
}

func (t *YandexDocsTransport) scheduleReconnect(attempt int, reasonCode string, cause error) {
	t.scheduleReconnectWithMinDelay(attempt, reasonCode, cause, 0)
}

func (t *YandexDocsTransport) scheduleReconnectWithMinDelay(attempt int, reasonCode string, cause error, minDelay time.Duration) {
	if !t.IsRunning() || attempt >= t.GetConfig().MaxReconnectAttempts {
		return
	}

	t.RecordReconnect()

	delay := t.backoffDelay(attempt)
	if delay < minDelay {
		delay = minDelay
	}
	causeText := strings.ReplaceAll(cause.Error(), "\n", " ")
	t.EmitEvent(transport.EventRetrying, fmt.Sprintf("%d|%d|%s|%s", attempt+1, int(delay.Seconds()), reasonCode, causeText))
	if delay > 0 {
		// A changed jar must take effect promptly, so the wait stays interruptible. Repeated
		// pushes of the same jar never reach here: ProvideCookies ignores an unchanged one.
		wake := make(chan struct{})
		t.Mu.Lock()
		t.wakeReconnect = wake
		t.Mu.Unlock()

		cutShort := false
		select {
		case <-time.After(delay):
		case <-wake:
			cutShort = true
			t.debugf("backoff wait cut short by ForceReconnect")
		}

		t.Mu.Lock()
		if t.wakeReconnect == wake {
			t.wakeReconnect = nil
		}
		// Only a cooldown that actually elapsed earns the next cookie-driven cut, otherwise a
		// cut would clear its own budget and the client could drive one retry per push again.
		if !cutShort {
			t.cookieCutPending = false
		}
		t.Mu.Unlock()
	}
	if !t.IsRunning() {
		return
	}

	t.connectToDoc(attempt + 1)
}

// ForceReconnect lets a caller that already knows the network changed skip waiting for a read to time out, since a network change often leaves the old socket silently dead rather than reset.
func (t *YandexDocsTransport) ForceReconnect() {
	t.Mu.Lock()
	session := t.session
	live := t.IsConnected()
	wake := t.wakeReconnect
	t.wakeReconnect = nil // claimed under the lock so a concurrent call can't double-close wake
	t.Mu.Unlock()

	if live && session != nil && session.Conn != nil {
		t.debugf("force-reconnect: dropping live session to re-dial")
		_ = session.Conn.Close()
		return
	}
	if wake != nil {
		close(wake)
	}
}

func (t *YandexDocsTransport) backoffDelay(attempt int) time.Duration {
	cfg := t.GetConfig()
	if cfg.ReconnectDelay <= 0 {
		return 0
	}

	multiplier := cfg.ReconnectMultiplier
	if multiplier < 1 {
		multiplier = 1
	}

	delay := float64(cfg.ReconnectDelay) * math.Pow(multiplier, float64(attempt))
	// +0-50% jitter is applied before the cap so many clients losing the same document at once don't retry in lockstep and pile up ghost participants.
	delay += delay * 0.5 * rand.Float64()
	if cfg.MaxReconnectDelay > 0 && delay > float64(cfg.MaxReconnectDelay) {
		delay = float64(cfg.MaxReconnectDelay)
	}
	return time.Duration(delay)
}

func (t *YandexDocsTransport) fetchDocInfo(url, userID string) (YandexDocsInfo, error) {
	jar, _ := cookiejar.New(nil)
	if parsed, err := neturl.Parse(url); err == nil {
		if provided := t.getProvidedCookies(); provided != "" {
			// Without an explicit root path the jar scopes every cookie to the directory of the
			// requested URL. A /i/ share link then sends the whole jar to /i/... but drops the
			// session cookies on the /edit/d/... redirect, which is answered with a captcha.
			cookies := parseCookieHeader(provided)
			for _, c := range cookies {
				if c.Path == "" {
					c.Path = "/"
				}
			}
			jar.SetCookies(parsed, cookies)
		}
	}

	client := &http.Client{
		Jar:       jar,
		Timeout:   30 * time.Second,
		Transport: &http.Transport{DialContext: transport.ProtectedDialer().DialContext},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	currentURL := url
	var resp *http.Response
	var htmlBytes []byte
	var finalCookies []*http.Cookie
	var finalURL *neturl.URL
	captchaAttempts := 0
	solveChallenge := func() error {
		if captchaAttempts > 0 {
			return errCaptchaBlocked
		}
		captchaAttempts++
		t.debugf("captcha challenge detected, solving via PoW")
		t.EmitEvent(transport.EventCaptchaRequired, url)
		if t.captchaSolveMode != CaptchaSolveModeNative {
			return errCaptchaBlocked
		}
		if cerr := solveCaptchaIsolated(currentURL, jar, browserUserAgent, client.Transport); cerr != nil {
			t.debugf("PoW captcha solve failed: %v", cerr)
			return fmt.Errorf("%w: %v", errCaptchaBlocked, cerr)
		}
		t.debugf("PoW captcha solved, retrying")
		currentURL = url
		return nil
	}

	for hop := 0; hop < 10; hop++ {
		req, _ := http.NewRequest("GET", currentURL, nil)
		applyBrowserGetHeaders(req.Header)
		sent := cookieNames(jar.Cookies(req.URL))
		t.debugf("hop %d GET %s (jar cookies: %s)", hop, shortURL(currentURL), sent)
		var err error
		resp, err = client.Do(req)
		if err != nil {
			return YandexDocsInfo{}, err
		}
		t.debugf("hop %d -> %d %s", hop, resp.StatusCode, resp.Header.Get("Location"))

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			htmlBytes, _ = io.ReadAll(resp.Body)
			finalCookies = resp.Cookies()
			finalURL = resp.Request.URL
			resp.Body.Close()
			if looksLikeCaptchaHTML(htmlBytes) {
				t.debugf("hop %d returned a captcha page (%d bytes, title %q)", hop, len(htmlBytes), pageTitle(htmlBytes))
				if err := solveChallenge(); err != nil {
					return YandexDocsInfo{}, err
				}
				continue
			}
			break
		}
		if resp.StatusCode < 300 || resp.StatusCode >= 400 {
			resp.Body.Close()
			return YandexDocsInfo{}, fmt.Errorf("unexpected status %d at %s", resp.StatusCode, currentURL)
		}

		loc, err := resp.Location()
		resp.Body.Close()
		if err != nil {
			return YandexDocsInfo{}, fmt.Errorf("redirect from %s: %w", currentURL, err)
		}

		nextURL, err := resp.Request.URL.Parse(loc.String())
		if err != nil {
			return YandexDocsInfo{}, fmt.Errorf("redirect from %s: %w", currentURL, err)
		}
		if isCaptchaURL(nextURL.String()) {
			if err := solveChallenge(); err != nil {
				return YandexDocsInfo{}, err
			}
			continue
		}
		currentURL = nextURL.String()
	}

	if resp == nil || len(htmlBytes) == 0 || finalURL == nil {
		if captchaAttempts > 0 {
			return YandexDocsInfo{}, errCaptchaBlocked
		}
		return YandexDocsInfo{}, fmt.Errorf("no successful response after redirects")
	}
	html := string(htmlBytes)

	t.debugf("fetchDocInfo GET %s -> %d (%d bytes)", url, resp.StatusCode, len(html))

	var cookies []string
	for _, c := range finalCookies {
		cookies = append(cookies, fmt.Sprintf("%s=%s", c.Name, c.Value))
	}
	for _, c := range jar.Cookies(finalURL) {
		cookies = append(cookies, fmt.Sprintf("%s=%s", c.Name, c.Value))
	}

	re := regexp.MustCompile(`<script[^>]*id="client-config"[^>]*>(.*?)</script>`)
	matches := re.FindStringSubmatch(html)
	if len(matches) < 2 {
		lower := strings.ToLower(html)
		isCaptcha := strings.Contains(lower, "captcha")
		if isCaptcha {
			t.debugf("response looks like a CAPTCHA/bot-check page, not the doc editor")
			t.EmitEvent(transport.EventCaptchaRequired, url)
		}
		preview := html
		if len(preview) > 2000 {
			preview = preview[:2000]
		}
		t.debugf("HTML preview: %s", preview)
		if isCaptcha {
			return YandexDocsInfo{}, errCaptchaBlocked
		}
		return YandexDocsInfo{}, fmt.Errorf("config not found")
	}

	if joined := strings.Join(cookies, "; "); joined != "" {
		merged := mergeCookieHeaders(joined, t.getProvidedCookies())
		t.Mu.Lock()
		t.providedCookies = merged
		t.Mu.Unlock()
	}

	var config map[string]interface{}
	if err := json.Unmarshal([]byte(matches[1]), &config); err != nil {
		return YandexDocsInfo{}, fmt.Errorf("parse client-config: %w", err)
	}

	// Every config lookup is a checked type assertion since this runs in a goroutine with no recover(), so an unchecked assertion would crash the process on an unexpected page shape.
	officeAction, ok := config["officeActionData"].(map[string]interface{})
	if !ok {
		t.debugf("config top-level keys: %v", mapKeys(config))
		return YandexDocsInfo{}, fmt.Errorf("officeActionData missing or malformed")
	}

	editorConfigRaw, ok := officeAction["editor_config"].(map[string]interface{})
	if !ok || editorConfigRaw == nil {
		t.debugf("officeActionData keys: %v", mapKeys(officeAction))
		return YandexDocsInfo{}, fmt.Errorf("editor_config nil - will reconnect")
	}

	balancerURL, ok := officeAction["balancer_url"].(string)
	if !ok {
		// officeActionData + editor_config present but balancer_url missing is the confirmed signature of a newer-generation document this transport can't talk to; only YandexVolgaTransport can.
		t.debugf("officeActionData keys: %v, editor_config keys: %v", mapKeys(officeAction), mapKeys(editorConfigRaw))
		return YandexDocsInfo{}, fmt.Errorf("balancer_url missing - this document looks like a newer Yandex Docs type this transport doesn't support; try the Volga transport for this doc_url instead")
	}
	host := strings.TrimPrefix(balancerURL, "https://")

	document, ok := editorConfigRaw["document"].(map[string]interface{})
	if !ok {
		t.debugf("editor_config keys: %v", mapKeys(editorConfigRaw))
		return YandexDocsInfo{}, fmt.Errorf("document missing or malformed")
	}

	token, ok := editorConfigRaw["token"].(string)
	if !ok {
		t.debugf("editor_config keys: %v", mapKeys(editorConfigRaw))
		return YandexDocsInfo{}, fmt.Errorf("editor_config.token missing or malformed")
	}

	docKey, ok := document["key"].(string)
	if !ok {
		t.debugf("document keys: %v", mapKeys(document))
		return YandexDocsInfo{}, fmt.Errorf("document.key missing or malformed")
	}

	perms, _ := document["permissions"].(map[string]interface{})
	if perms == nil {
		perms = make(map[string]interface{})
	}

	// editor_config.user.id — настоящий id участника в документе: под него
	// сервер подписывает presence/курсоры, и именно его нужно нести в
	// saveChanges (иначе auth-сессия и saveChanges разойдутся).
	editorUserID := ""
	if userObj, ok := editorConfigRaw["user"].(map[string]interface{}); ok {
		if s, ok := userObj["id"].(string); ok {
			editorUserID = s
		}
	}
	fileType, _ := document["fileType"].(string)
	isExcel := isExcelFile(fileType)

	return YandexDocsInfo{
		CookieStr:    strings.Join(cookies, "; "),
		Token:        token,
		DocID:        docKey,
		Origin:       balancerURL,
		Host:         host,
		EditorUserID: editorUserID,
		IsExcel:      isExcel,
		WsURL:        fmt.Sprintf("wss://%s/2024.1.1-375/doc/%s/c/?EIO=4&transport=websocket", host, docKey),
		Permissions:  perms,
		OpenCmd: map[string]interface{}{
			"c":      "open",
			"id":     docKey,
			"userid": userID,
			"format": document["fileType"],
			"url":    document["url"],
			"title":  document["title"],
			"lcid":   25,
		},
	}, nil
}

// isExcelFile — таблица ли это, по fileType из editor_config.document.
// От этого зависит поле isExcel в saveChanges.
func isExcelFile(fileType string) bool {
	switch strings.ToLower(strings.TrimPrefix(fileType, ".")) {
	case "xlsx", "xls", "xlsm", "csv":
		return true
	}
	return false
}

func randUserID() string {
	return fmt.Sprintf("%010d", rand.New(rand.NewSource(time.Now().UnixNano())).Intn(1000000000))
}
