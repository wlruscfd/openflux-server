package yandex

import (
	"math/rand"
	"time"
)

func reconnectBackoff(n int) time.Duration {
	if n < 1 {
		n = 1
	}
	shift := n - 1
	if shift > 4 {
		shift = 4
	}
	d := 1500 * time.Millisecond * time.Duration(1<<uint(shift))
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	d += time.Duration(rand.Int63n(int64(d/2) + 1))
	return d
}
