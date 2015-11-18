package oracles

import (
	"sync"
	"time"
)

type LocalOracle struct {
	mu              sync.Mutex
	lastTimeStampTs int64
	n               int64
}

func (l *LocalOracle) GetTimestamp() (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	ts := (time.Now().UnixNano() / int64(time.Millisecond)) << 18
	if l.lastTimeStampTs == ts {
		l.n++
		return uint64(ts + l.n), nil
	} else {
		l.lastTimeStampTs = ts
		l.n = 0
	}
	return uint64(ts), nil
}
