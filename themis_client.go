package themis

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	pb "github.com/golang/protobuf/proto"
	"github.com/ngaut/log"
	"github.com/pingcap/go-themis/hbase"
	"github.com/pingcap/go-themis/proto"
)

type themisClient interface {
	checkAndSetLockIsExpired(l ThemisLock, TTL uint64) (bool, error)
	themisGet(tbl []byte, g *hbase.Get, startTs uint64) (*hbase.ResultRow, error)
	prewriteRow(tbl []byte, row []byte, mutations []*columnMutation, prewriteTs uint64, primaryLockBytes []byte, secondaryLockBytes []byte, primaryOffset int) (ThemisLock, error)
	isLockExpired(tbl, row []byte, ts uint64) (bool, error)
	getLockAndErase(cc *hbase.ColumnCoordinate, prewriteTs uint64) (ThemisLock, error)
	commitRow(tbl, row []byte, mutations []*columnMutation, prewriteTs, commitTs uint64, primaryOffset int) error
	commitSecondaryRow(tbl, row []byte, mutations []*columnMutation, prewriteTs, commitTs uint64) error
	prewriteSecondaryRow(tbl, row []byte, mutations []*columnMutation, prewriteTs uint64, secondaryLockBytes []byte) (ThemisLock, error)
}

func newThemisClient(client hbaseClient) themisClient {
	return &themisClientImpl{
		client: client,
	}
}

type themisClientImpl struct {
	client hbaseClient
}

func (t *themisClientImpl) checkAndSetLockIsExpired(lock ThemisLock, TTL uint64) (bool, error) {
	b, err := t.isLockExpired(lock.getColumn().Table, lock.getColumn().Row, lock.getTimestamp())
	if err != nil {
		return false, err
	}
	lock.setExpired(b)
	return b, nil
}

func (t *themisClientImpl) themisGet(tbl []byte, g *hbase.Get, startTs uint64) (*hbase.ResultRow, error) {
	requestParam := &proto.ThemisGetRequest{
		Get:        g.ToProto().(*proto.Get),
		StartTs:    pb.Uint64(startTs),
		IgnoreLock: pb.Bool(false),
	}
	param, _ := pb.Marshal(requestParam)

	call := &hbase.CoprocessorServiceCall{
		Row:          g.GetRow(),
		ServiceName:  ThemisServiceName,
		MethodName:   "themisGet",
		RequestParam: param,
	}

	r, err := t.client.ServiceCall(string(tbl), call)
	if err != nil {
		return nil, err
	}
	var res proto.Result
	err = pb.Unmarshal(r.GetValue().GetValue(), &res)
	if err != nil {
		return nil, err
	}
	return hbase.NewResultRow(&res), nil
}

func (t *themisClientImpl) prewriteRow(tbl []byte, row []byte, mutations []*columnMutation, prewriteTs uint64, primaryLockBytes []byte, secondaryLockBytes []byte, primaryOffset int) (ThemisLock, error) {
	var cells []*proto.Cell
	request := &proto.ThemisPrewriteRequest{
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
	param, _ := pb.Marshal(request)
	call := &hbase.CoprocessorServiceCall{
		Row:          row,
		ServiceName:  ThemisServiceName,
		MethodName:   "prewriteRow",
		RequestParam: param,
	}

	r, err := t.client.ServiceCall(string(tbl), call)
	if err != nil {
		return nil, err
	}

	var res proto.ThemisPrewriteResponse
	err = pb.Unmarshal(r.GetValue().GetValue(), &res)
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
		return nil, errors.New("encounter conflict")
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
	req := &proto.LockExpiredRequest{
		Timestamp: pb.Uint64(ts),
	}
	param, _ := pb.Marshal(req)
	call := &hbase.CoprocessorServiceCall{
		Row:          row,
		ServiceName:  ThemisServiceName,
		MethodName:   "isLockExpired",
		RequestParam: param,
	}

	r, err := t.client.ServiceCall(string(tbl), call)
	if err != nil {
		return false, err
	}

	var res proto.LockExpiredResponse
	err = pb.Unmarshal(r.GetValue().GetValue(), &res)
	if err != nil {
		return false, err
	}
	return res.GetExpired(), nil
}

func (t *themisClientImpl) getLockAndErase(cc *hbase.ColumnCoordinate, prewriteTs uint64) (ThemisLock, error) {
	log.Info("rpc: getLockAndErase")
	req := &proto.EraseLockRequest{
		Row:        cc.Row,
		Family:     cc.Column.Family,
		Qualifier:  cc.Column.Qual,
		PrewriteTs: pb.Uint64(prewriteTs),
	}
	param, _ := pb.Marshal(req)
	call := &hbase.CoprocessorServiceCall{
		Row:          cc.Row,
		ServiceName:  ThemisServiceName,
		MethodName:   "getLockAndErase",
		RequestParam: param,
	}

	r, err := t.client.ServiceCall(string(cc.Table), call)
	if err != nil {
		return nil, err
	}

	var res proto.EraseLockResponse
	err = pb.Unmarshal(r.GetValue().GetValue(), &res)
	if err != nil {
		return nil, err
	}
	return parseLockFromBytes(res.GetLock())
}

func (t *themisClientImpl) commitRow(tbl, row []byte, mutations []*columnMutation, prewriteTs, commitTs uint64, primaryOffset int) error {
	req := &proto.ThemisCommitRequest{
		Row:          row,
		PrewriteTs:   pb.Uint64(prewriteTs),
		CommitTs:     pb.Uint64(commitTs),
		PrimaryIndex: pb.Int(primaryOffset),
	}
	for _, m := range mutations {
		req.Mutations = append(req.Mutations, m.toCell())
	}
	param, _ := pb.Marshal(req)
	call := &hbase.CoprocessorServiceCall{
		Row:          row,
		ServiceName:  ThemisServiceName,
		MethodName:   "commitRow",
		RequestParam: param,
	}

	r, err := t.client.ServiceCall(string(tbl), call)
	if err != nil {
		return err
	}

	var res proto.ThemisCommitResponse
	err = pb.Unmarshal(r.GetValue().GetValue(), &res)
	if err != nil {
		return err
	}

	ok := res.GetResult()
	if !ok {
		return errors.New(fmt.Sprintf("commit failed, tbl: %s row: %s ts: %d", tbl, row, commitTs))
	}
	return nil
}

func (t *themisClientImpl) commitSecondaryRow(tbl, row []byte, mutations []*columnMutation, prewriteTs, commitTs uint64) error {
	log.Info("rpc: commitSecondaryRow")
	return t.commitRow(tbl, row, mutations, prewriteTs, commitTs, -1)
}

func (t *themisClientImpl) prewriteSecondaryRow(tbl, row []byte, mutations []*columnMutation, prewriteTs uint64, secondaryLockBytes []byte) (ThemisLock, error) {
	return t.prewriteRow(tbl, row, mutations, prewriteTs, nil, secondaryLockBytes, -1)
}
