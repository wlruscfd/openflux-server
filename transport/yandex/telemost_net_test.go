package yandex

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"

	otransport "github.com/p1neappleXpress/OpenFlux/transport"
)

func TestTelemostICESocketsAreProtected(t *testing.T) {
	var protected atomic.Int32
	otransport.SetProtector(func(fd int) bool {
		protected.Add(1)
		return true
	})
	defer otransport.SetProtector(nil)

	api := webrtc.NewAPI(webrtc.WithSettingEngine(telemostSettingEngine()))
	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	if _, err := pc.CreateDataChannel("x", nil); err != nil {
		t.Fatal(err)
	}
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	done := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("ICE gathering did not finish")
	}
	if protected.Load() == 0 {
		t.Fatal("ICE opened sockets without asking the protector")
	}
}
