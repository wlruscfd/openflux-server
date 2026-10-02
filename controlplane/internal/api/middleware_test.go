package api

import (
	"net/http/httptest"
	"testing"
)

func TestClientIPUsesForwardedHeadersOnlyBehindLoopbackProxy(t *testing.T) {
	cases := []struct {
		name       string
		remoteAddr string
		realIP     string
		forwarded  string
		want       string
	}{
		{"direct client", "203.0.113.7:5555", "", "", "203.0.113.7"},
		{"direct client cannot spoof", "203.0.113.7:5555", "9.9.9.9", "8.8.8.8", "203.0.113.7"},
		{"loopback proxy with real ip", "127.0.0.1:4000", "198.51.100.2", "", "198.51.100.2"},
		{"loopback proxy with forwarded chain", "127.0.0.1:4000", "", "198.51.100.3, 10.0.0.1", "198.51.100.3"},
		{"loopback proxy without headers", "127.0.0.1:4000", "", "", "127.0.0.1"},
		{"loopback proxy with garbage header", "[::1]:4000", "not-an-ip", "", "::1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("POST", "/v1/keys/cookies", nil)
			r.RemoteAddr = tc.remoteAddr
			if tc.realIP != "" {
				r.Header.Set("X-Real-IP", tc.realIP)
			}
			if tc.forwarded != "" {
				r.Header.Set("X-Forwarded-For", tc.forwarded)
			}
			if got := clientIP(r); got != tc.want {
				t.Fatalf("clientIP = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestIPRateLimiterBurstThenBlocks(t *testing.T) {
	l := newIPRateLimiter(1, 3)

	for i := 0; i < 3; i++ {
		if !l.allow("1.2.3.4") {
			t.Fatalf("request %d within burst should be allowed", i)
		}
	}
	if l.allow("1.2.3.4") {
		t.Fatalf("request beyond burst should be rate limited")
	}
}

func TestIPRateLimiterTracksIndependently(t *testing.T) {
	l := newIPRateLimiter(1, 1)

	if !l.allow("1.1.1.1") {
		t.Fatalf("first request from 1.1.1.1 should be allowed")
	}
	if !l.allow("2.2.2.2") {
		t.Fatalf("first request from a different IP should be allowed independently")
	}
	if l.allow("1.1.1.1") {
		t.Fatalf("second immediate request from 1.1.1.1 should be limited")
	}
}
