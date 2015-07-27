package themis

import "sync"

type idGenerator struct {
	n  int
	mu *sync.RWMutex
}

func newIdGenerator() *idGenerator {
	return &idGenerator{
		n:  0,
		mu: &sync.RWMutex{},
	}
}

func (a *idGenerator) Get() int {
	a.mu.RLock()
	v := a.n
	a.mu.RUnlock()
	return v
}

func (a *idGenerator) IncrAndGet() int {
	a.mu.Lock()
	a.n++
	v := a.n
	a.mu.Unlock()
	return v
}
