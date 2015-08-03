package themis

import (
	"bytes"
	"testing"

	"github.com/ngaut/log"
	"github.com/pingcap/go-themis/hbase"
)

func TestMutationCache(t *testing.T) {
	cache := newColumnMutationCache()
	row := []byte("r1")
	col := &hbase.Column{[]byte("f1"), []byte("q1")}
	cache.addMutation([]byte("tbl"), row, col, hbase.TypePut, []byte("test"))
	cache.addMutation([]byte("tbl"), row, col, hbase.TypeDelete, []byte("test"))
	cache.addMutation([]byte("tbl"), row, col, hbase.TypePut, []byte("test"))

	cc := &hbase.ColumnCoordinate{
		Table: []byte("tbl"),
		Row:   []byte("r1"),
		Column: hbase.Column{
			Family: []byte("f1"),
			Qual:   []byte("q1"),
		},
	}
	mutation := cache.getMutation(cc)
	if mutation == nil || mutation.typ != hbase.TypePut || bytes.Compare(mutation.value, []byte("test")) != 0 {
		t.Error("cache error")
	} else {
		log.Info(mutation)
	}
}
