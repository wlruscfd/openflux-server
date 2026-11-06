package avito

import (
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/p1neappleXpress/OpenFlux/utils"
)

// wsPauseUntil is the moment before which no room of any key opens a connection. The provider
// limits connections per address, not per room: when one room is answered with 429 all of them
// are about to be, and every one retrying on its own timer meets the limit again together.
var wsPauseUntil atomic.Int64

// noteWSThrottle records a 429 and holds every room back for what the provider asked, plus a
// spread so the rooms do not all come back in the same instant.
func noteWSThrottle(after time.Duration) {
	until := time.Now().Add(after + time.Duration(rand.Int63n(int64(10*time.Second)))).UnixNano()
	for {
		old := wsPauseUntil.Load()
		if until <= old {
			return
		}
		if wsPauseUntil.CompareAndSwap(old, until) {
			if old < time.Now().UnixNano() {
				utils.Infof("[AVITO] the provider limits this address (429): all rooms wait %s", after.Round(time.Second))
			}
			return
		}
	}
}

// waitWSPause blocks until the pause is over; false when done closed first. Each room adds its own
// small spread, so the connections after the pause are not opened in the same instant.
func waitWSPause(done <-chan struct{}) bool {
	for {
		d := time.Until(time.Unix(0, wsPauseUntil.Load()))
		if d <= 0 {
			return true
		}
		t := time.NewTimer(d + time.Duration(rand.Int63n(int64(3*time.Second))))
		select {
		case <-t.C:
		case <-done:
			t.Stop()
			return false
		}
	}
}
