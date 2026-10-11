package yandex

import (
	"os"
	"strconv"

	"github.com/p1neappleXpress/OpenFlux/transport"
)

const (
	TelemostLanes    = 5
	TelemostMaxLanes = 11
)

func telemostLaneSettings() (lanes, maxLanes int, auto bool) {
	lanes, maxLanes = TelemostLanes, TelemostMaxLanes
	v := os.Getenv("OPENFLUX_TELEMOST_LANES")
	switch {
	case v == "auto":
		lanes, auto = 1, true
	case v != "":
		if n, err := strconv.Atoi(v); err == nil && n >= 1 {
			lanes = n
		}
	}
	if m, err := strconv.Atoi(os.Getenv("OPENFLUX_TELEMOST_MAX_LANES")); err == nil && m >= lanes {
		maxLanes = m
	} else if maxLanes < lanes {
		maxLanes = lanes
	}
	return lanes, maxLanes, auto
}

func NewTelemostStriped(conferenceURL string, isExitNode bool, config transport.TransportConfig) transport.Transport {
	lanes, maxLanes, auto := telemostLaneSettings()
	group := NewTelemostGroup("", conferenceURL, lanes, isExitNode, config)
	return transport.NewStripedTransportAuto(group.Lanes(), group, maxLanes, auto)
}

func NewTelemostBatched(conferenceURL string, isExitNode bool, config transport.TransportConfig) transport.Transport {
	return transport.NewBatchedTransport(NewTelemostStriped(conferenceURL, isExitNode, config))
}
