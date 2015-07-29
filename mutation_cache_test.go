package themis

import (
	"bytes"
	"testing"

	"github.com/ngaut/log"
)

func TestMutationCache(t *testing.T) {
	cache := newColumnMutationCache()
	row := []byte("r1")
	col := &column{[]byte("f1"), []byte("q1")}
	cache.addMutation([]byte("tbl"), row, col, TypePut, []byte("test"))
	cache.addMutation([]byte("tbl"), row, col, TypeDelete, []byte("test"))
	cache.addMutation([]byte("tbl"), row, col, TypePut, []byte("test"))

	cc := &columnCoordinate{
		table: []byte("tbl"),
		row:   []byte("r1"),
		column: column{
			family: []byte("f1"),
			qual:   []byte("q1"),
		},
	}
	mutation := cache.getMutation(cc)
	if mutation == nil || mutation.typ != TypePut || bytes.Compare(mutation.value, []byte("test")) != 0 {
		t.Error("cache error")
	} else {
		log.Info(mutation)
	}
}
