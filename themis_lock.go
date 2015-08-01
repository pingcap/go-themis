package themis

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/pingcap/go-themis/hbase"
	"github.com/pingcap/go-themis/iohelper"
)

var (
	_ ThemisLock = (*PrimaryLock)(nil)
	_ ThemisLock = (*SecondaryLock)(nil)
)

type lock struct {
	typ        Type
	ts         uint64
	wallTs     uint64
	clientAddr string
	expired    bool
	column     *hbase.ColumnCoordinate
}

func (l *lock) write(w io.Writer) {
	binary.Write(w, binary.BigEndian, byte(l.typ))
	binary.Write(w, binary.BigEndian, int64(l.ts))
	// write client addr
	iohelper.WriteVarBytes(w, []byte(l.clientAddr))
	binary.Write(w, binary.BigEndian, int64(l.wallTs))
}

type ThemisLock interface {
	getTimestamp() uint64
	setExpired(bool)
	isExpired() bool
	isPrimary() bool
	setColumn(col *hbase.ColumnCoordinate)
	getColumn() *hbase.ColumnCoordinate
	toBytes() []byte
	parseField(r iohelper.ByteMultiReader) error
}

func (l *lock) getTimestamp() uint64 {
	return l.ts
}

func (l *lock) parseField(r iohelper.ByteMultiReader) error {
	// read type
	var typ uint8
	err := binary.Read(r, binary.BigEndian, &typ)
	if err != nil {
		return err
	}
	l.typ = Type(typ)

	// read ts
	var ts int64
	err = binary.Read(r, binary.BigEndian, &ts)
	if err != nil {
		return err
	}
	l.ts = uint64(ts)

	// read client addr
	sz, err := binary.ReadUvarint(r)
	if err != nil {
		return err
	}
	addr := make([]byte, sz)
	r.Read(addr)
	l.clientAddr = string(addr)

	// read wall time
	var wallTs int64
	err = binary.Read(r, binary.BigEndian, &wallTs)
	if err != nil {
		return err
	}
	l.wallTs = uint64(wallTs)
	return nil
}

func (l *lock) setColumn(col *hbase.ColumnCoordinate) {
	l.column = col
}

func (l *lock) getColumn() *hbase.ColumnCoordinate {
	return l.column
}

func (l *lock) setExpired(b bool) {
	l.expired = b
}

func parseLockFromBytes(b []byte) (ThemisLock, error) {
	buf := bytes.NewBuffer(b)
	var isPrimary uint8
	err := binary.Read(buf, binary.BigEndian, &isPrimary)
	if err != nil {
		return nil, err
	}
	var ret ThemisLock
	if isPrimary == 1 {
		ret = newPrimaryLock()
	} else {
		ret = newSecondaryLock()
	}
	err = ret.parseField(buf)
	if err != nil {
		return nil, err
	}
	return ret, nil
}
