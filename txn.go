package themis

import (
	"errors"

	"github.com/pingcap/go-hbase"
)

var (
	ErrLockNotExpired  = errors.New("lock not expired")
	ErrCleanLockFailed = errors.New("clean lock failed")
)

type Txn interface {
	Get(t string, get *hbase.Get) (*hbase.ResultRow, error)
	Gets(t string, gets []*hbase.Get) ([]*hbase.ResultRow, error)
	LockRow(t string, row []byte) error
	Put(t string, put *hbase.Put)
	Delete(t string, del *hbase.Delete) error
	Commit() error
	GetStartTS() uint64
	GetCommitTS() uint64
}
