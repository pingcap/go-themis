package themis

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	pb "github.com/golang/protobuf/proto"
	"github.com/ngaut/log"
	"github.com/pingcap/go-themis/iohelper"
)

func processMessage(msg []byte) [][]byte {
	buf := pb.NewBuffer(msg)
	payloads := make([][]byte, 0)

	for {
		hbytes, err := buf.DecodeRawBytes(true)
		if err != nil {
			break
		}

		payloads = append(payloads, hbytes)
	}

	log.Debugf("Messages processed [n=%d]", len(payloads))

	return payloads
}

func readPayloads(r io.Reader) ([][]byte, error) {
	nBytesExpecting, err := iohelper.ReadInt32(r)
	if err != nil {
		return nil, err
	}

	if nBytesExpecting > 0 {
		buf, err := iohelper.ReadN(r, nBytesExpecting)

		if err != nil && err == io.EOF {
			return nil, err
		}

		payloads := processMessage(buf)

		if len(payloads) > 0 {
			return payloads, err
		}
	}
	return nil, errors.New("unexcepted payload")
}

func preparePayload(buf []byte) []byte {
	out := bytes.NewBuffer(nil)
	binary.Write(out, binary.BigEndian, int32(len(buf)))
	out.Write(buf)
	return out.Bytes()
}
