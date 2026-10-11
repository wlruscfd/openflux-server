package cupsonline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
)

func slowCursorNums(t *testing.T, raw []byte) (string, []uint64) {
	t.Helper()
	var obj map[string]interface{}
	if err := json.Unmarshal(raw, &obj); err != nil {
		t.Fatal(err)
	}
	payload := obj["push"].(map[string]interface{})["pub"].(map[string]interface{})["data"].(map[string]interface{})["payload"].(map[string]interface{})
	var nums []uint64
	for _, c := range payload["cursors"].([]interface{}) {
		m := c.(map[string]interface{})
		row, _ := asCursorInt(m["row"])
		col, _ := asCursorInt(m["column"])
		nums = append(nums, row, col)
	}
	return payload["user_uuid"].(string), nums
}

func TestAppendChunkMessageMatchesJSON(t *testing.T) {
	blob := make([]byte, 1000)
	rand.Read(blob)
	nums := packNumbers(blob)
	if len(nums)%2 == 0 {
		nums = nums[:len(nums)-1]
	}
	raw := appendChunkMessage(nil, nums, "room-1", "user-1", 42)
	var got map[string]interface{}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("not valid JSON: %v\n%s", err, raw)
	}
	data := got["rpc"].(map[string]interface{})["data"].(map[string]interface{})
	if data["room"] != "room-1" || data["user"] != "user-1" || got["id"].(float64) != 42 {
		t.Fatalf("envelope wrong: %s", raw)
	}
	var back []uint64
	for _, c := range data["cursors"].([]interface{}) {
		m := c.(map[string]interface{})
		r, _ := asCursorInt(m["row"])
		cl, _ := asCursorInt(m["column"])
		back = append(back, r, cl)
	}
	want := append(append([]uint64(nil), nums...), 0)
	if !reflect.DeepEqual(back, want) {
		t.Fatal("cursor numbers differ after the round trip")
	}
}

func TestParseCursorPushAgreesWithJSON(t *testing.T) {
	blob := make([]byte, 700)
	rand.Read(blob)
	nums := packNumbers(blob)
	if len(nums)%2 == 1 {
		nums = append(nums, 0)
	}
	var cur bytes.Buffer
	for i := 0; i < len(nums); i += 2 {
		if i > 0 {
			cur.WriteString(", ")
		}
		fmt.Fprintf(&cur, `{"row": %d, "column": %d}`, nums[i], nums[i+1])
	}
	forms := map[string]string{
		"compact": `{"push":{"channel":"c","pub":{"data":{"type":"cursors_update","payload":{"user_uuid":"u-1","cursors":[%s],"x":1}}}}}`,
		"spaced":  `{ "push" : { "channel" : "c", "pub" : { "data" : { "type" : "cursors_update", "payload" : { "cursors" : [ %s ], "user_uuid" : "u-1" } } } } }`,
	}
	for name, f := range forms {
		raw := []byte(fmt.Sprintf(f, cur.String()))
		wantSender, wantNums := slowCursorNums(t, raw)
		sender, got, st := parseCursorPush(raw)
		if st != pushParsed || sender != wantSender || !reflect.DeepEqual(got, wantNums) {
			t.Errorf("%s: status %d sender %q, %d vs %d numbers", name, st, sender, len(got), len(wantNums))
		}
	}
}

func TestParseCursorPushDefersWhatItDoesNotKnow(t *testing.T) {
	cases := map[string]struct {
		raw  string
		want int
	}{
		"other push":      {`{"push":{"pub":{"data":{"type":"user_joined"}}}}`, pushSkip},
		"float row":       {`{"t":"cursors_update","user_uuid":"u","cursors":[{"row":1.5,"column":2}]}`, pushFallback},
		"negative":        {`{"t":"cursors_update","user_uuid":"u","cursors":[{"row":-1,"column":2}]}`, pushFallback},
		"string column":   {`{"t":"cursors_update","user_uuid":"u","cursors":[{"row":1,"column":"x"}]}`, pushFallback},
		"no row":          {`{"t":"cursors_update","user_uuid":"u","cursors":[{"column":2}]}`, pushFallback},
		"escaped sender":  {`{"t":"cursors_update","user_uuid":"u\"x","cursors":[{"row":1,"column":2}]}`, pushFallback},
		"truncated":       {`{"t":"cursors_update","user_uuid":"u","cursors":[{"row":1,"colu`, pushFallback},
		"too many digits": {`{"t":"cursors_update","user_uuid":"u","cursors":[{"row":12345678901234567,"column":2}]}`, pushFallback},
		"empty":           {`{"t":"cursors_update","user_uuid":"u","cursors":[]}`, pushSkip},
		"duplicate key":   {`{"t":"cursors_update","user_uuid":"u","cursors":[{"row":1,"column":2}],"user_uuid":"v"}`, pushFallback},
	}
	for name, c := range cases {
		if _, _, st := parseCursorPush([]byte(c.raw)); st != c.want {
			t.Errorf("%s: status %d, want %d", name, st, c.want)
		}
	}
}

func BenchmarkReceiveGeneric(b *testing.B) {
	raw, _ := benchPush()
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var obj map[string]interface{}
		_ = json.Unmarshal(raw, &obj)
		payload := obj["push"].(map[string]interface{})["pub"].(map[string]interface{})["data"].(map[string]interface{})["payload"].(map[string]interface{})
		nums := make([]uint64, 0, 700)
		for _, c := range payload["cursors"].([]interface{}) {
			m := c.(map[string]interface{})
			r, _ := asCursorInt(m["row"])
			cl, _ := asCursorInt(m["column"])
			nums = append(nums, r, cl)
		}
		_ = bytesFromNumbers(nums)
	}
}

func BenchmarkReceiveFast(b *testing.B) {
	raw, _ := benchPush()
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, nums, _ := parseCursorPush(raw)
		_ = bytesFromNumbers(nums)
	}
}

func BenchmarkSendGeneric(b *testing.B) {
	blob := make([]byte, 4098)
	rand.Read(blob)
	for i := 0; i < b.N; i++ {
		msg := map[string]interface{}{"rpc": map[string]interface{}{"method": "m", "data": map[string]interface{}{"cursors": cursorsFromBytes(blob), "ranges": []interface{}{}, "room": "r", "user": "u"}}, "id": 1}
		_, _ = json.Marshal(msg)
	}
}

func BenchmarkSendFast(b *testing.B) {
	blob := make([]byte, 4098)
	rand.Read(blob)
	for i := 0; i < b.N; i++ {
		_ = appendChunkMessage(make([]byte, 0, 160+700*17), packNumbers(blob), "r", "u", 1)
	}
}

func benchPush() ([]byte, []uint64) {
	blob := make([]byte, 4098)
	rand.Read(blob)
	nums := packNumbers(blob)
	if len(nums)%2 == 1 {
		nums = append(nums, 0)
	}
	var cur bytes.Buffer
	for i := 0; i < len(nums); i += 2 {
		if i > 0 {
			cur.WriteByte(',')
		}
		fmt.Fprintf(&cur, `{"row":%d,"column":%d}`, nums[i], nums[i+1])
	}
	return []byte(fmt.Sprintf(`{"push":{"channel":"c","pub":{"data":{"type":"cursors_update","payload":{"user_uuid":"u","cursors":[%s]}}}}}`, cur.String())), nums
}

func TestSendRefusesPastTheQueueBound(t *testing.T) {
	w := &cupsWS{
		config:    CupsonlineConfig{SendQueueBytes: 10000},
		ctx:       context.Background(),
		sendQueue: make(chan []byte, 1024),
		stats:     &channelStats{},
	}
	pkt := make([]byte, 4000)
	for i := 0; i < 2; i++ {
		if err := w.Send(pkt); err != nil {
			t.Fatalf("packet %d refused with room left: %v", i, err)
		}
	}
	if err := w.Send(pkt); !errors.Is(err, errQueueFull) {
		t.Fatalf("a third packet must be refused, got %v", err)
	}
	if w.stats.dropped.Load() != 1 {
		t.Fatalf("dropped = %d, want 1", w.stats.dropped.Load())
	}
	<-w.sendQueue
	w.queuedBytes.Add(-4002)
	if err := w.Send(pkt); err != nil {
		t.Fatalf("a drained queue must accept again: %v", err)
	}
}
