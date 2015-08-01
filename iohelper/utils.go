package iohelper

import (
	"encoding/binary"
	"io"
)

func ReadVarBytes(r ByteMultiReader) ([]byte, error) {
	sz, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}
	b := make([]byte, sz)
	_, err = r.Read(b)
	return b, err
}

func WriteVarBytes(w io.Writer, b []byte) error {
	szBuf := make([]byte, 8)
	n := binary.PutUvarint(szBuf, uint64(len(b)))
	_, err := w.Write(szBuf[0:n])
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	if err != nil {
		return err
	}
	return nil
}

func ReadInt32(r io.Reader) (int32, error) {
	var n int32
	err := binary.Read(r, binary.BigEndian, &n)
	return n, err
}

func ReadN(r io.Reader, n int32) ([]byte, error) {
	b := make([]byte, n)
	_, err := io.ReadFull(r, b)
	return b, err
}
