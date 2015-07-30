package themis

import (
	"bytes"
	"encoding/binary"
	"errors"

	pb "github.com/golang/protobuf/proto"
	"github.com/pingcap/go-themis/proto"
)

type themisClient struct {
	client *Client
}

func (t *themisClient) themisGet(tbl []byte, g *Get, startTs uint64) (*ResultRow, error) {
	requestParam := &proto.ThemisGetRequest{
		Get:        g.toProto().(*proto.Get),
		StartTs:    pb.Uint64(startTs),
		IgnoreLock: pb.Bool(false),
	}
	param, _ := pb.Marshal(requestParam)

	call := &CoprocessorServiceCall{
		row:          g.key,
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
	return newResultRow(&res), nil
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
	// b[0]=>commitTs b[1] => lockbytes b[2]=>family b[3]=>qual b[4]=>isExpired
	// if b[0] != 0 means encounter conflict
	buf := bytes.NewBuffer(b[0])
	var commitTs int64
	if err := binary.Read(buf, binary.BigEndian, &commitTs); err != nil {
		return nil, err
	}
	if commitTs != 0 {
		return nil, errors.New("encounter conflict")
	}
	return nil, nil
}
