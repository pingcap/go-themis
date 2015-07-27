package themis

import (
	"time"

	pb "github.com/golang/protobuf/proto"
	"github.com/pingcap/go-themis/proto"
)

type Txn struct {
	c       *Client
	startTs uint64
}

func NewTxn(c *Client) *Txn {
	return &Txn{
		c:       c,
		startTs: uint64(time.Now().UnixNano()),
	}
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
		serviceName:  "ThemisService",
		methodName:   "themisGet",
		requestParam: param,
	}

	t.c.ServiceCall(string(tbl), call)
	return nil, nil
}
