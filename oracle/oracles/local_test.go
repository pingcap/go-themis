package oracles

import "testing"

func TestLocalOracle(t *testing.T) {
	l := &LocalOracle{}
	m := map[uint64]struct{}{}
	for i := 0; i < 100000; i++ {
		ts := l.GetTimestamp()
		m[ts] = struct{}{}
	}

	if len(m) != 100000 {
		t.Error("generated same ts")
	}
}
