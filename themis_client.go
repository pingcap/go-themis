package themis

import (
	"fmt"
	"runtime/debug"

	"github.com/juju/errors"

	pb "github.com/golang/protobuf/proto"
	"github.com/ngaut/log"
	"github.com/pingcap/go-hbase"
	"github.com/pingcap/go-hbase/proto"
	"github.com/pingcap/tidb/kv"
)

type themisClient interface {
	checkAndSetLockIsExpired(l ThemisLock) (bool, error)
	themisGet(tbl []byte, g *hbase.Get, startTs uint64, ignoreLock bool) (*hbase.ResultRow, error)
	themisBatchGet(tbl []byte, gets []*hbase.Get, startTs uint64, ignoreLock bool) ([]*hbase.ResultRow, error)
	prewriteRow(tbl []byte, row []byte, mutations []*columnMutation, prewriteTs uint64, primaryLockBytes []byte, secondaryLockBytes []byte, primaryOffset int) (ThemisLock, error)
	isLockExpired(tbl, row []byte, ts uint64) (bool, error)
	getLockAndErase(cc *hbase.ColumnCoordinate, prewriteTs uint64) (ThemisLock, error)
	commitRow(tbl, row []byte, mutations []*columnMutation, prewriteTs, commitTs uint64, primaryOffset int) error
	commitSecondaryRow(tbl, row []byte, mutations []*columnMutation, prewriteTs, commitTs uint64) error
	prewriteSecondaryRow(tbl, row []byte, mutations []*columnMutation, prewriteTs uint64, secondaryLockBytes []byte) (ThemisLock, error)
	batchPrewriteSecondaryRows(tbl []byte, rowM map[string]*rowMutation, prewriteTs uint64, secondaryLockBytes []byte) (map[string]ThemisLock, error)
	batchCommitSecondaryRows(tbl []byte, rowM map[string]*rowMutation, prewriteTs, commitTs uint64) error
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
		return errors.Trace(err)
	}
	err = pb.Unmarshal(r.GetValue().GetValue(), resp)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (t *themisClientImpl) checkAndSetLockIsExpired(lock ThemisLock) (bool, error) {
	b, err := t.isLockExpired(lock.getColumn().Table, lock.getColumn().Row, lock.getTimestamp())
	if err != nil {
		return false, errors.Trace(err)
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
		return nil, errors.Trace(err)
	}
	return hbase.NewResultRow(&resp), nil
}

func (t *themisClientImpl) themisBatchGet(tbl []byte, gets []*hbase.Get, startTs uint64, ignoreLock bool) ([]*hbase.ResultRow, error) {
	var protoGets []*proto.Get
	for _, g := range gets {
		protoGets = append(protoGets, g.ToProto().(*proto.Get))
	}
	req := &ThemisBatchGetRequest{
		Gets:       protoGets,
		StartTs:    pb.Uint64(startTs),
		IgnoreLock: pb.Bool(ignoreLock),
	}
	var resp ThemisBatchGetResponse
	err := t.call("themisBatchGet", tbl, gets[0].Row, req, &resp)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var results []*hbase.ResultRow
	for _, rs := range resp.GetRs() {
		results = append(results, hbase.NewResultRow(rs))
	}
	return results, nil
}

func (t *themisClientImpl) prewriteRow(tbl []byte, row []byte, mutations []*columnMutation, prewriteTs uint64, primaryLockBytes []byte, secondaryLockBytes []byte, primaryOffset int) (ThemisLock, error) {
	var cells []*proto.Cell
	request := &ThemisPrewriteRequest{}
	request.ThemisPrewrite = &ThemisPrewrite{
		Row:           row,
		PrewriteTs:    pb.Uint64(prewriteTs),
		PrimaryLock:   primaryLockBytes,
		SecondaryLock: secondaryLockBytes,
		PrimaryIndex:  pb.Int(primaryOffset),
	}
	if primaryLockBytes == nil {
		request.ThemisPrewrite.PrimaryLock = []byte("")
	}
	if secondaryLockBytes == nil {
		request.ThemisPrewrite.SecondaryLock = []byte("")
	}
	for _, m := range mutations {
		cells = append(cells, m.toCell())
	}
	request.ThemisPrewrite.Mutations = cells

	var res ThemisPrewriteResponse
	err := t.call("prewriteRow", tbl, row, request, &res)
	if err != nil {
		return nil, errors.Trace(err)
	}
	b := res.ThemisPrewriteResult
	if b == nil {
		// if lock is empty, means we got the lock, otherwise some one else had
		// locked this row, and the lock should return in rpc result
		return nil, nil
	}
	// Oops, someone else have already locked this row.

	commitTs := b.GetNewerWriteTs()
	if commitTs != 0 {
		log.Errorf("write conflict, encounter write with larger timestamp than prewriteTs=%d, commitTs=%d, row=%s", prewriteTs, commitTs, string(row))
		return nil, kv.ErrLockConflict
	}

	l, err := parseLockFromBytes(b.ExistLock)
	if err != nil {
		return nil, errors.Trace(err)
	}

	col := &hbase.ColumnCoordinate{
		Table: tbl,
		Row:   row,
		Column: hbase.Column{
			Family: b.Family,
			Qual:   b.Qualifier,
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
		return false, errors.Trace(err)
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
		return nil, errors.Trace(err)
	}
	b := res.GetLock()
	if len(b) == 0 {
		return nil, nil
	}
	return parseLockFromBytes(b)
}

func (t *themisClientImpl) commitRow(tbl, row []byte, mutations []*columnMutation,
	prewriteTs, commitTs uint64, primaryOffset int) error {
	req := &ThemisCommitRequest{}
	req.ThemisCommit = &ThemisCommit{
		Row:          row,
		PrewriteTs:   pb.Uint64(prewriteTs),
		CommitTs:     pb.Uint64(commitTs),
		PrimaryIndex: pb.Int(primaryOffset),
	}

	for _, m := range mutations {
		req.ThemisCommit.Mutations = append(req.ThemisCommit.Mutations, m.toCell())
	}
	var res ThemisCommitResponse
	err := t.call("commitRow", tbl, row, req, &res)
	if err != nil {
		return errors.Trace(err)
	}
	ok := res.GetResult()
	if !ok {
		return errors.New(fmt.Sprintf("commit failed, tbl: %s row: %s ts: %d", tbl, row, commitTs))
	}
	return nil
}

func (t *themisClientImpl) batchCommitSecondaryRows(tbl []byte, rowMs map[string]*rowMutation, prewriteTs, commitTs uint64) error {
	req := &ThemisBatchCommitSecondaryRequest{}

	i := 0
	var lastRow []byte
	req.ThemisCommit = make([]*ThemisCommit, len(rowMs))
	for row, rowM := range rowMs {
		var cells []*proto.Cell
		for col, m := range rowM.mutations {
			cells = append(cells, toCellFromRowM(col, m))
		}

		req.ThemisCommit[i] = &ThemisCommit{
			Row:          []byte(row),
			Mutations:    cells,
			PrewriteTs:   pb.Uint64(prewriteTs),
			CommitTs:     pb.Uint64(commitTs),
			PrimaryIndex: pb.Int(-1),
		}
		i++
		lastRow = []byte(row)
	}

	var res ThemisBatchCommitSecondaryResponse
	err := t.call("batchCommitSecondaryRows", tbl, lastRow, req, &res)
	if err != nil {
		return errors.Trace(err)
	}

	cResult := res.BatchCommitSecondaryResult
	if cResult != nil && len(cResult) > 0 {
		errorInfo := "commit failed, tbl:" + string(tbl)
		for _, r := range cResult {
			errorInfo += (" row:" + string(r.Row))
		}
		return errors.New(fmt.Sprintf("%s, commitTs:%d", errorInfo, commitTs))
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

func (t *themisClientImpl) batchPrewriteSecondaryRows(tbl []byte, rowMs map[string]*rowMutation, prewriteTs uint64, secondaryLockBytes []byte) (map[string]ThemisLock, error) {
	request := &ThemisBatchPrewriteSecondaryRequest{}
	request.ThemisPrewrite = make([]*ThemisPrewrite, len(rowMs))

	if secondaryLockBytes == nil {
		secondaryLockBytes = []byte("")
	}
	i := 0
	var lastRow []byte
	for row, rowM := range rowMs {
		var cells []*proto.Cell
		for col, m := range rowM.mutations {
			cells = append(cells, toCellFromRowM(col, m))
		}
		request.ThemisPrewrite[i] = &ThemisPrewrite{
			Row:           []byte(row),
			Mutations:     cells,
			PrewriteTs:    pb.Uint64(prewriteTs),
			PrimaryLock:   []byte(""),
			SecondaryLock: secondaryLockBytes,
			PrimaryIndex:  pb.Int(-1),
		}
		i++
		lastRow = []byte(row)
	}

	var res ThemisBatchPrewriteSecondaryResponse
	err := t.call("batchPrewriteSecondaryRows", tbl, lastRow, request, &res)
	if err != nil {
		return nil, errors.Trace(err)
	}

	//Perhaps, part row has not in a region, sample : when region split, then need try
	lockMap := make(map[string]ThemisLock)
	if res.RowsNotInRegion != nil && len(res.RowsNotInRegion) > 0 {
		for _, r := range res.RowsNotInRegion {
			tl, err := t.prewriteSecondaryRow(tbl, r, rowMs[string(r)].mutationList(true), prewriteTs, secondaryLockBytes)
			if err != nil {
				return nil, errors.Trace(err)
			}

			if tl != nil {
				lockMap[string(r)] = tl
			}
		}
	}

	b := res.ThemisPrewriteResult
	if b != nil && len(b) > 0 {
		for _, pResult := range b {
			lock, err := judgePerwriteResultRow(pResult, tbl, prewriteTs, pResult.Row)
			if err != nil {
				return nil, errors.Trace(err)
			}

			if lock != nil {
				lockMap[string(pResult.Row)] = lock
			}
		}
	}

	return lockMap, nil
}

func judgePerwriteResultRow(pResult *ThemisPrewriteResult, tbl []byte, prewriteTs uint64, row []byte) (ThemisLock, error) {
	// Oops, someone else have already locked this row.
	newerTs := pResult.GetNewerWriteTs()
	if newerTs != 0 {
		log.Errorf("write conflict, encounter write with larger timestamp than prewriteTs=%d, row=%s, conflict: newerTs=%d, row=%q",
			prewriteTs, string(row), newerTs, pResult.Row)
		return nil, kv.ErrLockConflict
	}

	l, err := parseLockFromBytes(pResult.ExistLock)
	if err != nil {
		return nil, errors.Trace(err)
	}
	col := &hbase.ColumnCoordinate{
		Table: tbl,
		Row:   row,
		Column: hbase.Column{
			Family: pResult.Family,
			Qual:   pResult.Qualifier,
		},
	}
	l.setColumn(col)
	return l, nil
}

func toCellFromRowM(col string, cvPair *mutationValuePair) *proto.Cell {
	c := &hbase.Column{}
	c.ParseFromString(col)
	ret := &proto.Cell{
		Family:    c.Family,
		Qualifier: c.Qual,
		Value:     cvPair.value,
	}
	if cvPair.typ == hbase.TypePut { // put
		ret.CellType = proto.CellType_PUT.Enum()
	} else if cvPair.typ == hbase.TypeMinimum { // onlyLock
		ret.CellType = proto.CellType_MINIMUM.Enum()
	} else { // delete, themis delete API only support delete column
		ret.CellType = proto.CellType_DELETE_COLUMN.Enum()
	}
	return ret
}
