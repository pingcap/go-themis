package oracle

import "time"

type Oracle interface {
	GetTimestamp() (uint64, error)
}

func IsExpired(beginMs uint64, TTL uint64) bool {
	return uint64(time.Now().UnixNano()/int64(time.Millisecond)) >= (beginMs + TTL)
}
