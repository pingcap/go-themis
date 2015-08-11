package themis

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime/debug"

	"github.com/c4pt0r/go-hbase"
	"github.com/c4pt0r/go-hbase/proto"
	pb "github.com/golang/protobuf/proto"
)

type themisClient interface {
	checkAndSetLockIsExpired(l ThemisLock) (bool, error)
	themisGet(tbl []byte, g *hbase.Get, startTs uint64, ignoreLock bool) (*hbase.ResultRow, error)
	prewriteRow(tbl []byte, row []byte, mutations []*columnMutation, prewriteTs uint64, primaryLockBytes []byte, secondaryLockBytes []byte, primaryOffset int) (ThemisLock, error)
	isLockExpired(tbl, row []byte, ts uint64) (bool, error)
	getLockAndErase(cc *hbase.ColumnCoordinate, prewriteTs uint64) (ThemisLock, error)
	commitRow(tbl, row []byte, mutations []*columnMutation, prewriteTs, commitTs uint64, primaryOffset int) error
	commitSecondaryRow(tbl, row []byte, mutations []*columnMutation, prewriteTs, commitTs uint64) error
	prewriteSecondaryRow(tbl, row []byte, mutations []*columnMutation, prewriteTs uint64, secondaryLockBytes []byte) (ThemisLock, error)
}

func newThemisClient(client hbase.HBaseClient) themisClient {
	return &themisClientImpl{
		client: client,
	}
}

type themisClientImpl struct {
	client hbase.HBaseClient
}

func (t *themisClientImpl) call(methodName string, tbl, row []byte, req pb.Message, resp pb.Message) error {
	param, _ := pb.Marshal(req)

	call := &hbase.CoprocessorServiceCall{
		Row:          row,
		ServiceName:  ThemisServiceName,
		MethodName:   methodName,
		RequestParam: param,
	}
	r, err := t.client.ServiceCall(string(tbl), call)
	if err != nil {
		return err
	}
	err = pb.Unmarshal(r.GetValue().GetValue(), resp)
	if err != nil {
		return err
	}
	return nil
}

func (t *themisClientImpl) checkAndSetLockIsExpired(lock ThemisLock) (bool, error) {
	b, err := t.isLockExpired(lock.getColumn().Table, lock.getColumn().Row, lock.getTimestamp())
	if err != nil {
		return false, err
	}
	lock.setExpired(b)
	return b, nil
}

func (t *themisClientImpl) themisGet(tbl []byte, g *hbase.Get, startTs uint64, ignoreLock bool) (*hbase.ResultRow, error) {
	req := &ThemisGetRequest{
		Get:        g.ToProto().(*proto.Get),
		StartTs:    pb.Uint64(startTs),
		IgnoreLock: pb.Bool(ignoreLock),
	}
	var resp proto.Result
	err := t.call("themisGet", tbl, g.Row, req, &resp)
	if err != nil {
		return nil, err
	}
	return hbase.NewResultRow(&resp), nil
}

func (t *themisClientImpl) prewriteRow(tbl []byte, row []byte, mutations []*columnMutation, prewriteTs uint64, primaryLockBytes []byte, secondaryLockBytes []byte, primaryOffset int) (ThemisLock, error) {
	var cells []*proto.Cell
	request := &ThemisPrewriteRequest{
		Row:           row,
		PrewriteTs:    pb.Uint64(prewriteTs),
		PrimaryLock:   primaryLockBytes,
		SecondaryLock: secondaryLockBytes,
		PrimaryIndex:  pb.Int(primaryOffset),
	}
	if primaryLockBytes == nil {
		request.PrimaryLock = []byte("")
	}
	if secondaryLockBytes == nil {
		request.SecondaryLock = []byte("")
	}
	for _, m := range mutations {
		cells = append(cells, m.toCell())
	}
	request.Mutations = cells

	var res ThemisPrewriteResponse
	err := t.call("prewriteRow", tbl, row, request, &res)
	if err != nil {
		return nil, err
	}
	b := res.GetResult()
	if b == nil {
		// if lock is empty, means we got the lock, otherwise some one else had
		// locked this row, and the lock should return in rpc result
		return nil, nil
	}
	// Oops, someone else have already locked this row.
	// the result: b[0]=>commitTs b[1] => lockbytes b[2]=>family b[3]=>qual b[4]=>isExpired
	buf := bytes.NewBuffer(b[0])
	var commitTs int64
	if err := binary.Read(buf, binary.BigEndian, &commitTs); err != nil {
		return nil, err
	}
	// if b[0] != 0 means encounter conflict
	if commitTs != 0 {
		return nil, fmt.Errorf("write conflict, encounter write with larger timestamp than prewriteTs=%d, commitTs=%d, row=%s", prewriteTs, commitTs, string(row))
	}
	l, err := parseLockFromBytes(b[1])
	if err != nil {
		return nil, err
	}
	col := &hbase.ColumnCoordinate{
		Table: tbl,
		Row:   row,
		Column: hbase.Column{
			Family: b[2],
			Qual:   b[3],
		},
	}
	l.setColumn(col)
	return l, nil
}

func (t *themisClientImpl) isLockExpired(tbl, row []byte, ts uint64) (bool, error) {
	req := &LockExpiredRequest{
		Timestamp: pb.Uint64(ts),
	}
	var res LockExpiredResponse
	if row == nil {
		debug.PrintStack()
	}
	err := t.call("isLockExpired", tbl, row, req, &res)
	if err != nil {
		return false, err
	}
	return res.GetExpired(), nil
}

func (t *themisClientImpl) getLockAndErase(cc *hbase.ColumnCoordinate, prewriteTs uint64) (ThemisLock, error) {
	req := &EraseLockRequest{
		Row:        cc.Row,
		Family:     cc.Column.Family,
		Qualifier:  cc.Column.Qual,
		PrewriteTs: pb.Uint64(prewriteTs),
	}
	var res EraseLockResponse
	err := t.call("getLockAndErase", cc.Table, cc.Row, req, &res)
	if err != nil {
		return nil, err
	}
	return parseLockFromBytes(res.GetLock())
}

func (t *themisClientImpl) commitRow(tbl, row []byte, mutations []*columnMutation,
	prewriteTs, commitTs uint64, primaryOffset int) error {
	req := &ThemisCommitRequest{
		Row:          row,
		PrewriteTs:   pb.Uint64(prewriteTs),
		CommitTs:     pb.Uint64(commitTs),
		PrimaryIndex: pb.Int(primaryOffset),
	}
	for _, m := range mutations {
		req.Mutations = append(req.Mutations, m.toCell())
	}
	var res ThemisCommitResponse
	err := t.call("commitRow", tbl, row, req, &res)
	if err != nil {
		return err
	}
	ok := res.GetResult()
	if !ok {
		return errors.New(fmt.Sprintf("commit failed, tbl: %s row: %s ts: %d", tbl, row, commitTs))
	}
	return nil
}

func (t *themisClientImpl) commitSecondaryRow(tbl, row []byte, mutations []*columnMutation,
	prewriteTs, commitTs uint64) error {
	return t.commitRow(tbl, row, mutations, prewriteTs, commitTs, -1)
}

func (t *themisClientImpl) prewriteSecondaryRow(tbl, row []byte,
	mutations []*columnMutation, prewriteTs uint64,
	secondaryLockBytes []byte) (ThemisLock, error) {
	return t.prewriteRow(tbl, row, mutations, prewriteTs, nil, secondaryLockBytes, -1)
}
