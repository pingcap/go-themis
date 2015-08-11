package themis

import (
	"bytes"
	"encoding/binary"

	"github.com/c4pt0r/go-hbase"
	"github.com/c4pt0r/go-hbase/iohelper"
)

type SecondaryLock struct {
	*lock
	primaryCoordinate *hbase.ColumnCoordinate
}

func newSecondaryLock() *SecondaryLock {
	return &SecondaryLock{
		lock: &lock{
			clientAddr: "null-client-addr",
		},
		primaryCoordinate: &hbase.ColumnCoordinate{},
	}
}

func (l *SecondaryLock) isExpired() bool {
	return l.lock.expired
}

func (l *SecondaryLock) getPrimaryColumn() *hbase.ColumnCoordinate {
	return l.primaryCoordinate
}

func (l *SecondaryLock) getPrimaryLock() ThemisLock {
	pl := newPrimaryLock()
	pl.setColumn(l.getPrimaryColumn())
	pl.ts = l.ts
	pl.clientAddr = l.clientAddr
	pl.addSecondaryColumn(l.getColumn(), l.typ)
	return pl
}

func (l *SecondaryLock) isPrimary() bool {
	return false
}

func (l *SecondaryLock) toBytes() []byte {
	buf := bytes.NewBuffer(nil)
	binary.Write(buf, binary.BigEndian, uint8(0))
	l.lock.write(buf)
	l.primaryCoordinate.Write(buf)
	return buf.Bytes()
}

func (l *SecondaryLock) parseField(r iohelper.ByteMultiReader) error {
	l.lock.parseField(r)
	primary := &hbase.ColumnCoordinate{}
	err := primary.ParseField(r)
	if err != nil {
		return err
	}
	l.primaryCoordinate = primary
	return nil
}
