package transport

import "testing"

type roomListInner struct {
	Transport
	list string
	cb   func(string)
}

func (r *roomListInner) RoomList() string { return r.list }

func (r *roomListInner) OnRoomList(f func(string)) { r.cb = f }

func (r *roomListInner) publish(list string) {
	r.list = list
	if r.cb != nil {
		r.cb(list)
	}
}

func TestBatchedTransportExposesInnerRoomList(t *testing.T) {
	inner := &roomListInner{}
	batched := NewBatchedTransport(inner)

	var got string
	var subscribed bool
	batched.OnRoomList(func(list string) {
		subscribed = true
		got = list
	})

	inner.publish("room-a,room-b")

	if !subscribed {
		t.Fatal("the wrapper did not forward the room list subscription to the inner transport")
	}
	if got != "room-a,room-b" {
		t.Fatalf("got %q want %q", got, "room-a,room-b")
	}
	if listed := batched.RoomList(); listed != "room-a,room-b" {
		t.Fatalf("RoomList() = %q want %q", listed, "room-a,room-b")
	}
}

func TestBatchedTransportRoomListEmptyWithoutInnerSupport(t *testing.T) {
	inner := &plainInner{}
	batched := NewBatchedTransport(inner)

	batched.OnRoomList(func(string) {})

	if listed := batched.RoomList(); listed != "" {
		t.Fatalf("RoomList() = %q want empty", listed)
	}
}

type plainInner struct {
	Transport
}
