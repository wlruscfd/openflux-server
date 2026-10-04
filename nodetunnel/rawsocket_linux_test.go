package nodetunnel

import "testing"

func TestRawReaderGoroutinesStaysWithinBounds(t *testing.T) {
	n := rawReaderGoroutines()
	if n < 2 || n > 8 {
		t.Fatalf("rawReaderGoroutines() = %d, want between 2 and 8", n)
	}
}

func TestRawSocketCoreActivePortsRoutesToOwningWorker(t *testing.T) {
	core := &rawSocketCore{}
	a := &RawSocketEndpoint{core: core}
	b := &RawSocketEndpoint{core: core}

	core.activePorts.Store(uint16(40000), a)
	core.activePorts.Store(uint16(40001), b)

	v, ok := core.activePorts.Load(uint16(40000))
	if !ok || v.(*RawSocketEndpoint) != a {
		t.Fatalf("port 40000 should route to worker a")
	}
	v, ok = core.activePorts.Load(uint16(40001))
	if !ok || v.(*RawSocketEndpoint) != b {
		t.Fatalf("port 40001 should route to worker b")
	}
}

// Close must remove only the calling worker's own port registrations - a shared core means another worker's entries in the same map must survive a sibling's Close.
func TestRawSocketEndpointCloseOnlySweepsItsOwnPorts(t *testing.T) {
	core := &rawSocketCore{}
	a := &RawSocketEndpoint{core: core}
	b := &RawSocketEndpoint{core: core}

	core.activePorts.Store(uint16(40000), a)
	core.activePorts.Store(uint16(40001), a)
	core.activePorts.Store(uint16(50000), b)

	a.Close()

	if _, ok := core.activePorts.Load(uint16(40000)); ok {
		t.Errorf("port 40000 (worker a) should have been removed by a.Close()")
	}
	if _, ok := core.activePorts.Load(uint16(40001)); ok {
		t.Errorf("port 40001 (worker a) should have been removed by a.Close()")
	}
	if v, ok := core.activePorts.Load(uint16(50000)); !ok || v.(*RawSocketEndpoint) != b {
		t.Errorf("port 50000 (worker b) should survive a.Close(), got ok=%v", ok)
	}
}
