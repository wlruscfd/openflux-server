package oneme

import (
	"fmt"

	"github.com/p1neappleXpress/OpenFlux/transport"
	"github.com/p1neappleXpress/OpenFlux/utils"
)

type OneMeTransport struct {
	b     *transport.BaseTransport
	token string
	uid   int64
	exit  bool

	oneMeClient MaxClient
	ch          *CallHandler
}

func (t *OneMeTransport) Receive(callback func([]byte)) {
	t.b.Receive(callback)
}

func (t *OneMeTransport) Stats() transport.TransportStats {
	return t.b.Stats()
}

func (t *OneMeTransport) SetEventCallback(fn func(code, detail string)) {
	t.b.SetEventCallback(fn)
}

func (t *OneMeTransport) ForceReconnect() {
	if t.ch == nil {
		return
	}
	t.ch.signalReconnect()
}

func NewOneMeTransport(isExit bool, maxToken string, maxUid int64, config transport.TransportConfig) *OneMeTransport {
	return &OneMeTransport{
		b:     transport.NewBaseTransport(config),
		token: maxToken,
		uid:   maxUid,
		exit:  isExit,
	}
}

func (t *OneMeTransport) Start() error {
	t.b.EmitEvent(transport.EventConnecting, "1")

	utils.Debugf("creating max client ...")
	t.oneMeClient = *NewMaxClient()
	if err := t.oneMeClient.Connect(); err != nil {
		return fmt.Errorf("max: connect: %w", err)
	}
	if err := t.oneMeClient.LoginByToken(t.token); err != nil {
		return fmt.Errorf("max: login: %w", err)
	}

	if t.exit {
		utils.Debugf("configured ch for exit node")
		t.ch = startIncomingListener(&t.oneMeClient)
	} else {
		utils.Debugf("configured ch for client mode")
		t.ch = startOutgoingCall(&t.oneMeClient, t.uid)
	}

	utils.Debugf("configured dc inbound")
	t.ch.dcInbound = func(data []byte) {
		t.b.CallReceive(data)
	}
	t.b.EmitEvent(transport.EventConnected, "1")

	return t.b.Start()
}

func (t *OneMeTransport) Stop() error {
	return t.b.Stop()
}

func (t *OneMeTransport) IsConnected() bool {
	return true
}

func (t *OneMeTransport) Send(data []byte) error {
	t.ch.Send(data)
	return nil
}
