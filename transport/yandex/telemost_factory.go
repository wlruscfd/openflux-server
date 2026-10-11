package yandex

import "github.com/p1neappleXpress/OpenFlux/transport"

const (
	TelemostLanes    = 5
	TelemostMaxLanes = 11
)

func NewTelemostStriped(conferenceURL string, isExitNode bool, config transport.TransportConfig) transport.Transport {
	group := NewTelemostGroup("", conferenceURL, TelemostLanes, isExitNode, config)
	return transport.NewStripedTransportAuto(group.Lanes(), group, TelemostMaxLanes, false)
}

func NewTelemostBatched(conferenceURL string, isExitNode bool, config transport.TransportConfig) transport.Transport {
	return transport.NewBatchedTransport(NewTelemostStriped(conferenceURL, isExitNode, config))
}
