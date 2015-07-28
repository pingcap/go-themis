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
