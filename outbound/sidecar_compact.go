package outbound

import (
	"github.com/p1neappleXpress/OpenFlux/utils"
)

// compactSparseShare is the share of a process's keys that has to be wanted for it to be worth
// keeping. A process holds a port, a listener and an outbound for every key it was started with,
// wanted or not, so one live key in a process of three hundred costs all of it.
const compactSparseShare = 4

// compactLocked retires processes that mostly hold keys nobody wants any more. The wanted keys of
// such a process are taken out of have, so the caller starts them again in a fresh process; only
// those few keys lose their open connections, the rest of the set is not touched. When the process
// limit is reached the sparsest process is retired even if it is not that sparse, because without a
// free slot nothing new can start at all.
func (s *Sidecar) compactLocked(ordered []Key, wanted map[string]string, have map[string]Dialer) {
	fresh := 0
	for _, k := range ordered {
		if have[k.ID] == nil {
			fresh++
		}
	}
	for {
		var worst *segment
		worstLive, worstTotal := 0, 0
		for _, seg := range s.segments {
			live := 0
			for id, fp := range seg.keys {
				if wanted[id] == fp && have[id] == seg.dialers[id] {
					live++
				}
			}
			total := len(seg.keys)
			if total == 0 || live == 0 {
				continue
			}
			if worst == nil || live*worstTotal < worstLive*total {
				worst, worstLive, worstTotal = seg, live, total
			}
		}
		if worst == nil {
			return
		}
		sparse := worstLive*compactSparseShare < worstTotal
		full := len(s.segments) >= maxSegments && fresh+worstLive > 0
		if !sparse && !full {
			return
		}
		moved := 0
		for id, fp := range worst.keys {
			if wanted[id] == fp && have[id] == worst.dialers[id] {
				delete(have, id)
				moved++
			}
		}
		fresh += moved
		utils.Infof("[OUTBOUND] compacting sing-box segment %d: %d of %d keys still wanted, moving them", worst.id, moved, worstTotal)
		s.killSegment(worst)
		live := s.segments[:0]
		for _, seg := range s.segments {
			if seg != worst {
				live = append(live, seg)
			}
		}
		s.segments = live
		if !sparse {
			return
		}
	}
}
