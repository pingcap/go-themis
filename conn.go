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
	addr     string
	conn     net.Conn
	idGen    *idGenerator
	isMaster bool
}

func newConnection(addr string, isMaster bool) (*connection, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	c := &connection{
		addr:     addr,
		conn:     conn,
		idGen:    newIdGenerator(),
		isMaster: isMaster,
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
	c.processMessages()
	return nil
}

func (c *connection) processMessages() {
	for {
		msg, err := readPayload(c.conn)
		if err != nil {
			panic(err)
		}
		if msg == nil {
			continue
		}
		log.Info(msg)
		var rh proto.ResponseHeader
		err = pb.Unmarshal(msg, &rh)
		if err != nil {
			panic(err)
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
	buf := bytes.NewBuffer(nil)
	service := pb.String("ClientService")
	if c.isMaster {
		service = pb.String("MasterService")
	}

	b, err := pb.Marshal(&proto.ConnectionHeader{
		UserInfo: &proto.UserInformation{
			EffectiveUser: pb.String("pingcap"),
		},
		ServiceName: service,
	})
	if err != nil {
		return err
	}

	_, err = buf.Write(b)
	if err != nil {
		return err
	}

	// out payload :  size | msg buffer
	outBuf := preparePayload(buf.Bytes())

	n, err := c.conn.Write(outBuf)
	if err != nil {
		return err
	}
	log.Info("outgoing", n)
	return nil
}

func (c *connection) call(request *call) error {
	id := c.idGen.IncrAndGet()
	rh := &proto.RequestHeader{
		CallId:       pb.Uint32(uint32(id)),
		MethodName:   pb.String(request.methodName),
		RequestParam: pb.Bool(true),
	}

	request.setid(uint32(id))

	bfrh := newOutputBuffer()
	err := bfrh.WritePBMessage(rh)
	if err != nil {
		panic(err)
	}

	bfr := newOutputBuffer()
	err = bfr.WritePBMessage(request.request)
	if err != nil {
		panic(err)
	}

	buf := newOutputBuffer()
	buf.writeDelimitedBuffers(bfrh, bfr)

	c.calls[id] = request
	n, err := c.conn.Write(buf.Bytes())

	if err != nil {
		return err
	}

	log.Debugf("Sent bytes to server [callId=%d] [n=%d] [connection=%s]", id, n, c.name)

	if n != len(buf.Bytes()) {
		return fmt.Errorf("Sent bytes not match number bytes [n=%d] [actual_n=%d]", n, len(buf.Bytes()))
	}

	return nil
}
