package themis

import (
	"bytes"
	"fmt"
	"net"

	pb "github.com/golang/protobuf/proto"
	"github.com/ngaut/log"
	"github.com/pingcap/go-themis/proto"
)

type connection struct {
	addr         string
	conn         net.Conn
	idGen        *idGenerator
	isMaster     bool
	ongoingCalls map[int]*call
}

func newConnection(addr string, isMaster bool) (*connection, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	c := &connection{
		addr:         addr,
		conn:         conn,
		idGen:        newIdGenerator(),
		isMaster:     isMaster,
		ongoingCalls: map[int]*call{},
	}
	err = c.init()
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *connection) init() error {
	err := c.writeHead()
	if err != nil {
		return err
	}
	err = c.writeConnectionHeader()
	if err != nil {
		return err
	}
	go c.processMessages()
	return nil
}

func (c *connection) processMessages() {
	for {
		msgs, err := readPayloads(c.conn)

		var rh proto.ResponseHeader
		err = pb.Unmarshal(msgs[0], &rh)
		if err != nil {
			panic(err)
		}

		callId := rh.GetCallId()
		call, ok := c.ongoingCalls[int(callId)]
		if !ok {
			log.Error(fmt.Errorf("Invalid call id: %d", callId))
		}

		delete(c.ongoingCalls, int(callId))

		exception := rh.GetException()
		if exception != nil {
			call.complete(fmt.Errorf("Exception returned: %s\n%s", exception.GetExceptionClassName(), exception.GetStackTrace()), nil)
		} else if len(msgs) == 2 {
			call.complete(nil, msgs[1])
		}
	}
}

func (c *connection) writeHead() error {
	buf := bytes.NewBuffer(nil)
	buf.Write(hbaseHeaderBytes)
	buf.WriteByte(0)
	buf.WriteByte(80)
	_, err := c.conn.Write(buf.Bytes())
	return err
}

func (c *connection) writeConnectionHeader() error {
	buf := newOutputBuffer()
	service := pb.String("ClientService")
	if c.isMaster {
		service = pb.String("MasterService")
	}

	err := buf.WritePBMessage(&proto.ConnectionHeader{
		UserInfo: &proto.UserInformation{
			EffectiveUser: pb.String("pingcap"),
		},
		ServiceName: service,
	})
	if err != nil {
		return err
	}

	err = buf.PrependSize()
	if err != nil {
		return err
	}

	_, err = c.conn.Write(buf.Bytes())
	if err != nil {
		return err
	}

	return nil
}

func (c *connection) call(request *call) error {
	id := c.idGen.IncrAndGet()
	rh := &proto.RequestHeader{
		CallId:       pb.Uint32(uint32(id)),
		MethodName:   pb.String(request.methodName),
		RequestParam: pb.Bool(true),
	}

	request.id = uint32(id)

	bfrh := newOutputBuffer()
	err := bfrh.WritePBMessage(rh)
	if err != nil {
		return err
	}

	bfr := newOutputBuffer()
	err = bfr.WritePBMessage(request.request)
	if err != nil {
		return err
	}

	buf := newOutputBuffer()
	//Buf=> | total size | pb1 size| pb1 size | pb2 size | pb2 | ...
	buf.writeDelimitedBuffers(bfrh, bfr)

	c.ongoingCalls[id] = request
	n, err := c.conn.Write(buf.Bytes())

	if err != nil {
		return err
	}

	if n != len(buf.Bytes()) {
		return fmt.Errorf("Sent bytes not match number bytes [n=%d] [actual_n=%d]", n, len(buf.Bytes()))
	}
	return nil
}
