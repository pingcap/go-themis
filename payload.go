package themis

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	pb "github.com/golang/protobuf/proto"
	"github.com/ngaut/log"
)

func readInt32(r io.Reader) (int32, error) {
	var n int32
	err := binary.Read(r, binary.BigEndian, &n)
	return n, err
}

func readN(r io.Reader, n int32) ([]byte, error) {
	b := make([]byte, n)
	_, err := io.ReadFull(r, b)
	return b, err
}

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
	nBytesExpecting, err := readInt32(r)
	if err != nil {
		return nil, err
	}

	if nBytesExpecting > 0 {
		buf, err := readN(r, nBytesExpecting)

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
