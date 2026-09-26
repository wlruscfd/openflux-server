package mts

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"universal-bypass-tool/transport"
)

const samplePage = `<script type="module" defer charset="utf-8">
    const baseInfo = {
        accountId:  0 ,
        accountName: "Guest",
        token: "7da2c6e1-d415-4c80-afdc-f0da03cae2ad",
        boardUID: "57a1dbd3-3590-4658-942e-580dd85b0cc7",
        boardName: "gg",
        access: "view_mini",
        navigationMode: "trackpad",
        fePrefix: "2026-09-21.178998474",
        boardAccessToken: "",
        appDomain: "my.mts-link.ru/boards",
        isSSL:  true ,
        wsDomain: "wss://wsboard.mts-link.ru/boards",
        reserveWsDomain: "wss://wsboard2.mts-link.ru/boards",
        needPassword:  false ,
        temporary:  true ,
        clientUID: "35d0a0b1-6a1e-4a1a-9c2f-2f0a4a6b1c33",
        signature: "eyJhbGciOiJIUzI1NiJ9.eyJ0ZW1wb3JhbnkiOnRydWV9.sig",
    };
</script>`

func newTestTransport(sink *[][]byte) *Transport {
	tr := NewTransport("", transport.DefaultConfig())
	tr.Receive(func(b []byte) { *sink = append(*sink, b) })
	return tr
}

func testSession(t *testing.T) *mtsSession {
	t.Helper()
	return &mtsSession{Info: mtsInfo{token: "tok", guestName: "guest_test"}}
}

func TestExtractBoardUID(t *testing.T) {
	cases := map[string]string{
		"https://my.mts-link.ru/boards/board/57a1dbd3-3590-4658-942e-580dd85b0cc7":     "57a1dbd3-3590-4658-942e-580dd85b0cc7",
		"https://my.mts-link.ru/boards/board/57a1dbd3-3590-4658-942e-580dd85b0cc7/":    "57a1dbd3-3590-4658-942e-580dd85b0cc7",
		"https://my.mts-link.ru/boards/board/57a1dbd3-3590-4658-942e-580dd85b0cc7?x=1": "57a1dbd3-3590-4658-942e-580dd85b0cc7",
		"https://my.mts-link.ru/boards/":                                               "",
		"":                                                                             "",
	}
	for in, want := range cases {
		if got := extractBoardUID(in); got != want {
			t.Errorf("extractBoardUID(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestScrapeGuestIdentity(t *testing.T) {
	if got := jsStringField(samplePage, "token"); got != "7da2c6e1-d415-4c80-afdc-f0da03cae2ad" {
		t.Errorf("token = %q", got)
	}
	if got := jsStringField(samplePage, "wsDomain"); got != "wss://wsboard.mts-link.ru/boards" {
		t.Errorf("wsDomain = %q", got)
	}
	if got := jsStringField(samplePage, "reserveWsDomain"); got != "wss://wsboard2.mts-link.ru/boards" {
		t.Errorf("reserveWsDomain = %q", got)
	}
	if got := jsStringField(samplePage, "appDomain"); got != "my.mts-link.ru/boards" {
		t.Errorf("appDomain = %q", got)
	}
	if got := jsStringField(samplePage, "boardAccessToken"); got != "" {
		t.Errorf("boardAccessToken = %q, want empty", got)
	}
	if !jsBoolField(samplePage, "temporary") {
		t.Error("temporary should be true")
	}
	if jsBoolField(samplePage, "needPassword") {
		t.Error("needPassword should be false")
	}
	if jsBoolField(samplePage, "missingKey") {
		t.Error("missingKey should be false")
	}
}

func TestHandleViewDeliversForeignPacket(t *testing.T) {
	mine := "11111111-1111-4111-8111-111111111111"
	sess := testSession(t)
	sess.sessionUID.Store(&mine)

	var got [][]byte
	tr := newTestTransport(&got)

	payload := []byte("tunnel-packet-bytes")
	encoded := base64.StdEncoding.EncodeToString(payload)
	msg := `{"type":"fast","subtype":"view","data":{"sessionUID":"22222222-2222-4222-8222-222222222222",` +
		`"cursorPosition":{"x":"` + encoded + `","y":123},"viewPosition":{}}}`
	tr.handleMessage(sess, []byte(msg))

	if len(got) != 1 || string(got[0]) != string(payload) {
		t.Fatalf("expected one payload %q, got %#v", payload, got)
	}
}

func TestHandleViewUnbatchesBatchFrame(t *testing.T) {
	mine := "11111111-1111-4111-8111-111111111111"
	sess := testSession(t)
	sess.sessionUID.Store(&mine)

	var got [][]byte
	tr := newTestTransport(&got)

	frame := transport.EncodeBatch([][]byte{[]byte("one"), []byte("two"), []byte("three")})
	encoded := base64.StdEncoding.EncodeToString(frame)
	msg := `{"type":"fast","subtype":"view","data":{"sessionUID":"22222222-2222-4222-8222-222222222222",` +
		`"cursorPosition":{"x":"` + encoded + `","y":123}}}`
	tr.handleMessage(sess, []byte(msg))

	if len(got) != 3 {
		t.Fatalf("expected 3 packets, got %d: %q", len(got), got)
	}
	for i, want := range []string{"one", "two", "three"} {
		if string(got[i]) != want {
			t.Errorf("packet %d = %q, want %q", i, got[i], want)
		}
	}
}

func TestHandleViewIgnoresOwnEchoJunkAndBoardState(t *testing.T) {
	mine := "11111111-1111-4111-8111-111111111111"
	sess := testSession(t)
	sess.sessionUID.Store(&mine)

	var got [][]byte
	tr := newTestTransport(&got)

	echo := `{"type":"fast","subtype":"view","data":{"sessionUID":"` + mine + `","cursorPosition":{"x":"` +
		base64.StdEncoding.EncodeToString([]byte("self")) + `","y":123}}}`
	notBase64 := `{"type":"fast","subtype":"view","data":{"sessionUID":"22222222-2222-4222-8222-222222222222","cursorPosition":{"x":"!!!not base64!!!","y":123}}}`
	otherSubtype := `{"type":"fast","subtype":"select","data":{"sessionUID":"22222222-2222-4222-8222-222222222222","selectedGroups":[]}}`
	notJSON := `<html>error</html>`
	desk := `{"type":"descResponse","status":"success","data":"{\"entities\":[]}"` + strings.Repeat(`,"x":1`, 5000) + `}"`
	users := `{"type":"boardUsersResponse","status":"success","data":{"users":[{"name":"Guest"}]}}`

	for _, m := range []string{echo, notBase64, otherSubtype, notJSON, desk, users} {
		tr.handleMessage(sess, []byte(m))
	}
	if len(got) != 0 {
		t.Fatalf("expected no deliveries, got %#v", got)
	}
}

func TestWriterLoopCoalescesIntoOneFrame(t *testing.T) {
	frames := make(chan []byte, 16)
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		for {
			_, msg, err := c.ReadMessage()
			if err != nil {
				return
			}
			frames <- msg
		}
	}))
	defer srv.Close()

	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	sessionUID := "33333333-3333-4333-8333-333333333333"
	sess := &mtsSession{Info: mtsInfo{token: "tok", guestName: "guest_test"}, Conn: conn}
	sess.sessionUID.Store(&sessionUID)

	tr := NewTransport("", transport.DefaultConfig())
	tr.session.Store(sess)
	go tr.writerLoop()
	defer tr.Stop()

	const packets = 20
	for i := 0; i < packets; i++ {
		pkt := []byte(strings.Repeat(string(rune('a'+i)), 64))
		if err := tr.Send(pkt); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}

	var delivered [][]byte
	frameCount := 0
	deadline := time.After(10 * time.Second)
collect:
	for {
		select {
		case raw := <-frames:
			frameCount++
			var env struct {
				Subtype string `json:"subtype"`
				Data    struct {
					CursorPosition struct {
						X string `json:"x"`
					} `json:"cursorPosition"`
				} `json:"data"`
			}
			if err := json.Unmarshal(raw, &env); err != nil {
				t.Fatalf("frame is not JSON: %v", err)
			}
			if env.Subtype != "view" {
				t.Fatalf("unexpected subtype %q", env.Subtype)
			}
			frame, err := base64.StdEncoding.DecodeString(env.Data.CursorPosition.X)
			if err != nil {
				t.Fatalf("cursor payload is not base64: %v", err)
			}
			if !transport.IsBatchFrame(frame) {
				t.Fatalf("cursor payload is not a batch frame: first byte 0x%02x", frame[0])
			}
			pkts, err := transport.DecodeBatch(frame)
			if err != nil {
				t.Fatalf("decode batch: %v", err)
			}
			delivered = append(delivered, pkts...)
			if len(delivered) >= packets {
				break collect
			}
		case <-deadline:
			t.Fatalf("timed out with %d/%d packets delivered", len(delivered), packets)
		}
	}

	drainTimer := time.NewTimer(300 * time.Millisecond)
drain:
	for {
		select {
		case <-frames:
			frameCount++
		case <-drainTimer.C:
			break drain
		}
	}
	drainTimer.Stop()

	for i := 0; i < packets; i++ {
		want := strings.Repeat(string(rune('a'+i)), 64)
		if string(delivered[i]) != want {
			t.Fatalf("packet %d = %q, want %q", i, delivered[i], want)
		}
	}
	if frameCount >= packets {
		t.Fatalf("%d packets went out as %d frames, expected coalescing", packets, frameCount)
	}
	t.Logf("%d packets coalesced into %d cursor frames", packets, frameCount)
}

func TestFlushStashesWhileDisconnected(t *testing.T) {
	tr := NewTransport("", transport.DefaultConfig())
	defer tr.Stop()

	pkts := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	tr.flush(pkts, 5)

	tr.stashMu.Lock()
	stashed := len(tr.stash)
	tr.stashMu.Unlock()
	if stashed != len(pkts) {
		t.Fatalf("stashed %d packets, want %d", stashed, len(pkts))
	}

	big := make([]byte, mtsStashBytes)
	tr.flush([][]byte{big}, len(big))
	tr.stashMu.Lock()
	after := tr.stashLen
	tr.stashMu.Unlock()
	if after > mtsStashBytes {
		t.Fatalf("stash grew to %d bytes, cap is %d", after, mtsStashBytes)
	}
}

func TestStashConcurrentAccess(t *testing.T) {
	tr := NewTransport("", transport.DefaultConfig())
	defer tr.Stop()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				tr.stashPackets([][]byte{[]byte("x")}, 1)
				tr.stashMu.Lock()
				tr.stash = nil
				tr.stashLen = 0
				tr.stashMu.Unlock()
			}
		}()
	}
	wg.Wait()
}
