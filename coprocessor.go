package themis

import (
	"errors"
	"fmt"

	pb "github.com/golang/protobuf/proto"
	"github.com/ngaut/log"
	"github.com/pingcap/go-themis/proto"
)

type CoprocessorServiceCall struct {
	row          []byte
	serviceName  string
	methodName   string
	requestParam []byte
}

func (c *CoprocessorServiceCall) ToProto() pb.Message {
	return &proto.CoprocessorServiceCall{
		Row:         c.row,
		ServiceName: pb.String(c.serviceName),
		MethodName:  pb.String(c.methodName),
		Request:     c.requestParam,
	}
}

func (cli *Client) ServiceCall(table string, call *CoprocessorServiceCall) (*proto.CoprocessorServiceResponse, error) {
	ch := cli.action([]byte(table), call.row, call, true, 0)
	response := <-ch
	switch r := response.(type) {
	case *proto.CoprocessorServiceResponse:
		return r, nil
	case *exception:
		log.Error(r.msg)
		return nil, errors.New(r.msg)
	}

	return nil, fmt.Errorf("No valid response seen [response: %#v]", response)
}
