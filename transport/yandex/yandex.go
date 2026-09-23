package yandex

import (
	"bytes"
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
	neturl "net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"golang.org/x/crypto/chacha20poly1305"

	"universal-bypass-tool/transport"
	"universal-bypass-tool/utils"
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

// errCaptchaBlocked marks fetchDocInfo failures where Yandex served a CAPTCHA/bot-check page
// instead of the doc editor - retrying on the normal fast backoff only reinforces a block like
// this, so connectToDoc gives it a much longer, fixed cooldown instead (see captchaCooldown).
var errCaptchaBlocked = errors.New("captcha or bot-check page returned instead of the doc editor")

// captchaCooldown is a floor, not the usual attempt-scaled backoff: a CAPTCHA means this
// client/IP is already flagged, so hammering it faster just extends the block.
const captchaCooldown = 3 * time.Minute

type YandexDocsInfo struct {
	CookieStr   string
	Token       string
	DocID       string
	CallbackURL string
	UserID      string
	Origin      string
	Host        string
	WsURL       string
	Permissions map[string]interface{}
	OpenCmd     map[string]interface{}
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
}

func (t *YandexDocsTransport) debugf(format string, args ...interface{}) {
	utils.Debugf("[YDOCS/%s] "+format, append([]interface{}{t.tag}, args...)...)
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

	select {
	case session.WriteQueue <- data:
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

		info, err := t.fetchDocInfo(t.url, userID)
		if err != nil {
			t.debugf("fetchDocInfo failed: %v", err)
			if errors.Is(err, errCaptchaBlocked) {
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

// scheduleReconnectWithMinDelay is scheduleReconnect with a floor under the usual attempt-scaled
// backoff, for failures (like a CAPTCHA) where the normal fast retry schedule is actively
// counterproductive rather than just slow.
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
		wake := make(chan struct{})
		t.Mu.Lock()
		t.wakeReconnect = wake
		t.Mu.Unlock()

		select {
		case <-time.After(delay):
		case <-wake:
			t.debugf("backoff wait cut short by ForceReconnect")
		}

		t.Mu.Lock()
		if t.wakeReconnect == wake {
			t.wakeReconnect = nil
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
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error { return nil },
		Timeout:       30 * time.Second,
		Transport:     &http.Transport{DialContext: transport.ProtectedDialer().DialContext},
	}

	req, _ := http.NewRequest("GET", url, nil)
	applyBrowserGetHeaders(req.Header)
	resp, err := client.Do(req)
	if err != nil {
		return YandexDocsInfo{}, err
	}
	defer resp.Body.Close()

	htmlBytes, _ := io.ReadAll(resp.Body)
	html := string(htmlBytes)

	t.debugf("fetchDocInfo GET %s -> %d (%d bytes)", url, resp.StatusCode, len(html))

	var cookies []string
	for _, c := range resp.Cookies() {
		cookies = append(cookies, fmt.Sprintf("%s=%s", c.Name, c.Value))
	}

	re := regexp.MustCompile(`<script[^>]*id="client-config"[^>]*>(.*?)</script>`)
	matches := re.FindStringSubmatch(html)
	if len(matches) < 2 {
		lower := strings.ToLower(html)
		isCaptcha := strings.Contains(lower, "captcha")
		if isCaptcha {
			t.debugf("response looks like a CAPTCHA/bot-check page, not the doc editor")
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

	return YandexDocsInfo{
		CookieStr:   strings.Join(cookies, "; "),
		Token:       token,
		DocID:       docKey,
		Origin:      balancerURL,
		Host:        host,
		WsURL:       fmt.Sprintf("wss://%s/2024.1.1-375/doc/%s/c/?EIO=4&transport=websocket", host, docKey),
		Permissions: perms,
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

func randUserID() string {
	return fmt.Sprintf("%010d", rand.New(rand.NewSource(time.Now().UnixNano())).Intn(1000000000))
}
