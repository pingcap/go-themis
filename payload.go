package themis

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	pb "github.com/golang/protobuf/proto"
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

func readPayload(r io.Reader) ([]byte, error) {
	nBytesExpecting, err := readInt32(r)
	if err != nil {
		return nil, err
	}
	if nBytesExpecting > 0 {
		buf, err := readN(r, nBytesExpecting)
		if err != nil {
			return nil, err
		}
		msgBuf := pb.NewBuffer(buf)
		hbytes, err := msgBuf.DecodeRawBytes(true)
		if err != nil {
			return nil, err
		}
		return hbytes, nil
	}
	return nil, errors.New("unexcepted payload")
}

func preparePayload(buf []byte) []byte {
	out := bytes.NewBuffer(nil)
	binary.Write(out, binary.BigEndian, int32(len(buf)))
	out.Write(buf)
	return out.Bytes()
}
