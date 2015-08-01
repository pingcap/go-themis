package themis

import (
	"bytes"
	"encoding/binary"
	"errors"

	pb "github.com/golang/protobuf/proto"
	"github.com/pingcap/go-themis/hbase"
	"github.com/pingcap/go-themis/proto"
)

type themisClient struct {
	client *Client
}

func (t *themisClient) checkAndSetLockIsExpired(lock ThemisLock, TTL uint64) (bool, error) {
	b, err := t.isLockExpired(lock.getColumn().Table, lock.getColumn().Row, lock.getTimestamp())
	if err != nil {
		return false, err
	}
	lock.setExpired(b)
	return b, nil
}

func (t *themisClient) themisGet(tbl []byte, g *hbase.Get, startTs uint64) (*hbase.ResultRow, error) {
	requestParam := &proto.ThemisGetRequest{
		Get:        g.ToProto().(*proto.Get),
		StartTs:    pb.Uint64(startTs),
		IgnoreLock: pb.Bool(false),
	}
	param, _ := pb.Marshal(requestParam)

	call := &CoprocessorServiceCall{
		row:          g.GetRow(),
		serviceName:  ThemisServiceName,
		methodName:   "themisGet",
		requestParam: param,
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

func (t *themisClient) prewriteRow(tbl []byte, row []byte, mutations []*columnMutation, prewriteTs uint64, primaryLockBytes []byte, secondaryLockBytes []byte, primaryOffset int) (ThemisLock, error) {
	var cells []*proto.Cell
	request := &proto.ThemisPrewriteRequest{
		Row:           row,
		PrewriteTs:    pb.Uint64(prewriteTs),
		PrimaryLock:   primaryLockBytes,
		SecondaryLock: secondaryLockBytes,
		PrimaryIndex:  pb.Int(primaryOffset),
	}
	for _, m := range mutations {
		cells = append(cells, m.toCell())
	}
	request.Mutations = cells
	param, _ := pb.Marshal(request)
	call := &CoprocessorServiceCall{
		row:          row,
		serviceName:  ThemisServiceName,
		methodName:   "prewriteRow",
		requestParam: param,
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

func (t *themisClient) isLockExpired(tbl, row []byte, ts uint64) (bool, error) {
	req := &proto.LockExpiredRequest{
		Timestamp: pb.Uint64(ts),
	}
	param, _ := pb.Marshal(req)
	call := &CoprocessorServiceCall{
		row:          row,
		serviceName:  ThemisServiceName,
		methodName:   "isLockExpired",
		requestParam: param,
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
