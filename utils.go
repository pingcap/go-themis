package themis

import (
	"bytes"
	"encoding/binary"
	"io"
)

func isContainPreservedColumn(family, qualifier []byte) bool {
	// cannot contain #
	for _, b := range family {
		if b == '#' {
			return true
		}
	}
	// check if lock column family
	if bytes.Compare(family, LockFamilyName) == 0 {
		return true
	}
	// check if put column family
	if bytes.Compare(family, PutFamilyName) == 0 {
		return true
	}
	// check if del column family
	if bytes.Compare(family, DelFamilyName) == 0 {
		return true
	}
	return false
}

type ByteMultiReader interface {
	io.ByteReader
	io.Reader
}

func readVarBytes(r ByteMultiReader) ([]byte, error) {
	sz, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}
	b := make([]byte, sz)
	_, err = r.Read(b)
	return b, err
}

func writeVarBytes(w io.Writer, b []byte) error {
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
