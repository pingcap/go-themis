package themis

import "github.com/pingcap/go-hbase"

type LockRole int

func (l LockRole) String() string {
	if l == RolePrimary {
		return "primary"
	}
	return "secondary"
}

const (
	RolePrimary LockRole = iota
	RoleSecondary
)

type Lock interface {
	SetCoordinate(c *hbase.ColumnCoordinate)
	Coordinate() *hbase.ColumnCoordinate
	Timestamp() uint64
	SetExpired(b bool)
	IsExpired() bool
	Type() hbase.Type
	Role() LockRole
	// not used now
	Context() interface{}
	// valid only  Role == Primary
	Secondaries() []Lock
	Primary() Lock
	Encode() []byte
}

type LockManager interface {
	// CleanLock if clean lock success, first return value is transction's commit
	// timestamp, otherwise, the second return value is transction's primary
	// lock.
	CleanLock(c *hbase.ColumnCoordinate, prewriteTs uint64) (uint64, Lock, error)
	// EraseLockAndData removes lock and data.
	EraseLockAndData(c *hbase.ColumnCoordinate, prewriteTs uint64) error
	// GetCommitTimestamp returns a committed transction's commit timestamp.
	GetCommitTimestamp(c *hbase.ColumnCoordinate, prewriteTs uint64) (uint64, error)
	// [startTs, endTs]
	IsLockExists(c *hbase.ColumnCoordinate, startTs, endTs uint64) (bool, error)
}
