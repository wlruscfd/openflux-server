package nodeagent

import "testing"

func TestWarpFingerprintAndDrift(t *testing.T) {
	plain := RemoteKey{ID: "k", Transport: "yandex", DocURL: "u"}
	if warpFingerprint(plain) != "" {
		t.Fatal("a key without WARP has no fingerprint")
	}
	viaWarp := plain
	viaWarp.Warp = &RemoteWarp{ProfileID: "p1", PrivateKey: "a", PeerPublicKey: "b", Endpoint: "1.2.3.4:2408", AddressV4: "172.16.0.2"}
	fp := warpFingerprint(viaWarp)
	if fp == "" {
		t.Fatal("a WARP key needs a fingerprint")
	}

	w := &worker{transportName: "yandex", docURL: "u", warpFP: ""}
	if !w.driftedFrom(viaWarp) {
		t.Fatal("switching a running key to WARP must restart it")
	}
	w.warpFP = fp
	if w.driftedFrom(viaWarp) {
		t.Fatal("an unchanged WARP key must not restart")
	}
	if !w.driftedFrom(plain) {
		t.Fatal("switching a running key back to a direct exit must restart it")
	}
	other := viaWarp
	other.Warp = &RemoteWarp{ProfileID: "p2", PrivateKey: "a", PeerPublicKey: "b", Endpoint: "1.2.3.4:2408", AddressV4: "172.16.0.2"}
	if !w.driftedFrom(other) {
		t.Fatal("another WARP profile must restart the key")
	}

	relayed := viaWarp
	host, port := "h", 41000
	relayed.RelayHost, relayed.RelayPort = &host, &port
	if warpFingerprint(relayed) != "" {
		t.Fatal("a cascaded key relays instead of using WARP")
	}
}
