package oracle

import (
	"testing"
	"time"
)

func TestExpired(t *testing.T) {
	beginMs := time.Now().UnixNano() / int64(time.Millisecond)
	time.Sleep(1 * time.Second)
	if !IsExpired(uint64(beginMs), 500) {
		t.Error("should expired")
	}
	if IsExpired(uint64(beginMs), 2000) {
		t.Error("should not expired")
	}
}
