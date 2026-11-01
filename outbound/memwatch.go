package outbound

import (
	"os"
	"strconv"
	"strings"
)

// memLowPercent is the share of memory that has to stay available. Below it the sidecar stops
// the processes nobody is assigned to instead of waiting out their drain period: the kernel
// kills the largest process when memory runs out, and on a node that process is the node itself.
const memLowPercent = 15

// memLow reports whether available memory is under memLowPercent. Where /proc/meminfo cannot
// be read it says false, so a platform without it never has processes stopped early.
func memLow() bool {
	raw, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return false
	}
	var total, avail int64
	for _, line := range strings.Split(string(raw), "\n") {
		f := strings.Fields(line)
		if len(f) < 2 {
			continue
		}
		v, err := strconv.ParseInt(f[1], 10, 64)
		if err != nil {
			continue
		}
		switch f[0] {
		case "MemTotal:":
			total = v
		case "MemAvailable:":
			avail = v
		}
	}
	return total > 0 && avail*100/total < memLowPercent
}
