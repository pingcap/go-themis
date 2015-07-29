package themis

import (
	"bytes"

	pb "github.com/golang/protobuf/proto"
	"github.com/ngaut/log"
	"github.com/pingcap/go-themis/oracle"
	"github.com/pingcap/go-themis/oracle/oracles"
	"github.com/pingcap/go-themis/proto"
)

type Txn struct {
	c             *Client
	oracle        oracle.Oracle
	mutationCache *columnMutationCache
	startTs       uint64
}

func NewTxn(c *Client) *Txn {
	txn := &Txn{
		c:             c,
		mutationCache: newColumnMutationCache(),
		oracle:        &oracles.LocalOracle{},
	}
	txn.startTs = txn.oracle.GetTimestamp()
	return txn
}

func isLockResult(r *ResultRow) bool {
	if len(r.SortedColumns) > 0 && isLockColumn(r.SortedColumns[0]) {
		return true
	}
	return false
}

func isLockColumn(c *ResultRowColumn) bool {
	if bytes.Compare(c.Family, LockFamilyName) == 0 {
		return true
	}
	return false
}

func shouldClean(l ThemisLock) bool {
	// TODO check worker alive
	return l.IsExpired()
}

func cleanLock(l ThemisLock) {
}

func tryCleanLock(table string, lockKvs *ResultRow) error {
	for _, c := range lockKvs.SortedColumns {
		if isLockColumn(c) {
			l, err := parseLockFromBytes([]byte(c.Value))
			if err != nil {
				return err
			}
			log.Info(l.IsPrimary())
		}
	}
	return nil
}

func (t *Txn) Get(tbl string, g *ThemisGet) (*ResultRow, error) {
	requestParam := &proto.ThemisGetRequest{
		Get:        g.get.toProto().(*proto.Get),
		StartTs:    pb.Uint64(t.startTs),
		IgnoreLock: pb.Bool(false),
	}
	param, _ := pb.Marshal(requestParam)

	call := &CoprocessorServiceCall{
		row:          g.get.key,
		serviceName:  ThemisServiceName,
		methodName:   "themisGet",
		requestParam: param,
	}

	r, err := t.c.ServiceCall(string(tbl), call)
	if err != nil {
		return nil, err
	}
	rr := newResultRow(r.(*proto.Result))
	if isLockResult(rr) {
		tryCleanLock(tbl, rr)
	}

	return nil, nil
}

func (txn *Txn) Put(tbl string, p *ThemisPut) {
	for _, e := range p.put.Entries() {
		txn.mutationCache.addMutation([]byte(tbl), p.put.key, e.column, e.typ, e.value)
	}
}
