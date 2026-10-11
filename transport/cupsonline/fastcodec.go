package cupsonline

import (
	"bytes"
	"strconv"
)

const (
	pushParsed = iota
	pushSkip
	pushFallback
)

var (
	tagCursorsUpdate = []byte(`"cursors_update"`)
	tagUserUUID      = []byte(`"user_uuid"`)
	tagCursors       = []byte(`"cursors"`)
)

func appendChunkMessage(dst []byte, nums []uint64, room, user string, id int64) []byte {
	dst = append(dst, `{"rpc":{"method":"shared_editor_change_cursors","data":{"cursors":[`...)
	for i := 0; i < len(nums); i += 2 {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = append(dst, `{"row":`...)
		dst = strconv.AppendUint(dst, nums[i], 10)
		dst = append(dst, `,"column":`...)
		if i+1 < len(nums) {
			dst = strconv.AppendUint(dst, nums[i+1], 10)
		} else {
			dst = append(dst, '0')
		}
		dst = append(dst, '}')
	}
	dst = append(dst, `],"ranges":[],"room":"`...)
	dst = append(dst, room...)
	dst = append(dst, `","user":"`...)
	dst = append(dst, user...)
	dst = append(dst, `"}},"id":`...)
	dst = strconv.AppendInt(dst, id, 10)
	return append(dst, '}')
}

func skipSpace(b []byte, i int) int {
	for i < len(b) && (b[i] == ' ' || b[i] == '\n' || b[i] == '\t' || b[i] == '\r') {
		i++
	}
	return i
}

func valueAfterKey(raw []byte, tag []byte) (int, bool) {
	idx := bytes.Index(raw, tag)
	if idx < 0 || bytes.LastIndex(raw, tag) != idx {
		return 0, false
	}
	i := skipSpace(raw, idx+len(tag))
	if i >= len(raw) || raw[i] != ':' {
		return 0, false
	}
	return skipSpace(raw, i+1), true
}

// parseCursorPush reads a cursors_update push without building a generic
// JSON tree: it carries the whole data stream, hundreds of numbers per
// message. Anything it does not recognise is left to the generic decoder.
func parseCursorPush(raw []byte) (sender string, nums []uint64, status int) {
	if !bytes.Contains(raw, tagCursorsUpdate) {
		return "", nil, pushSkip
	}
	i, ok := valueAfterKey(raw, tagUserUUID)
	if !ok || i >= len(raw) || raw[i] != '"' {
		return "", nil, pushFallback
	}
	end := bytes.IndexByte(raw[i+1:], '"')
	if end < 0 || bytes.IndexByte(raw[i+1:i+1+end], '\\') >= 0 {
		return "", nil, pushFallback
	}
	sender = string(raw[i+1 : i+1+end])

	i, ok = valueAfterKey(raw, tagCursors)
	if !ok || i >= len(raw) || raw[i] != '[' {
		return "", nil, pushFallback
	}
	i++
	nums = make([]uint64, 0, min(len(raw)/12, 4096))
	for {
		i = skipSpace(raw, i)
		if i >= len(raw) {
			return "", nil, pushFallback
		}
		switch raw[i] {
		case ']':
			if len(nums) == 0 {
				return "", nil, pushSkip
			}
			return sender, nums, pushParsed
		case ',':
			i++
			continue
		case '{':
			i++
		default:
			return "", nil, pushFallback
		}
		var row, col uint64
		haveRow := false
		for {
			i = skipSpace(raw, i)
			if i >= len(raw) {
				return "", nil, pushFallback
			}
			if raw[i] == '}' {
				i++
				break
			}
			if raw[i] == ',' {
				i++
				continue
			}
			if raw[i] != '"' {
				return "", nil, pushFallback
			}
			j := bytes.IndexByte(raw[i+1:], '"')
			if j < 0 {
				return "", nil, pushFallback
			}
			key := raw[i+1 : i+1+j]
			i = skipSpace(raw, i+j+2)
			if i >= len(raw) || raw[i] != ':' {
				return "", nil, pushFallback
			}
			i = skipSpace(raw, i+1)
			var v uint64
			digits := 0
			for i < len(raw) && raw[i] >= '0' && raw[i] <= '9' {
				v = v*10 + uint64(raw[i]-'0')
				digits++
				i++
			}
			if digits == 0 || digits > 16 {
				return "", nil, pushFallback
			}
			if i < len(raw) && (raw[i] == '.' || raw[i] == 'e' || raw[i] == 'E') {
				return "", nil, pushFallback
			}
			switch string(key) {
			case "row":
				row, haveRow = v, true
			case "column":
				col = v
			}
		}
		if !haveRow {
			return "", nil, pushFallback
		}
		nums = append(nums, row, col)
	}
}
