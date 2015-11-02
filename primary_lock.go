package themis

import (
	"bytes"
	"encoding/binary"

	"github.com/juju/errors"
	"github.com/pingcap/go-hbase"
	"github.com/pingcap/go-hbase/iohelper"
)

type PrimaryLock struct {
	*lock
	// {coordinate => type}
	secondaries map[string]hbase.Type
}

func newPrimaryLock() *PrimaryLock {
	return &PrimaryLock{
		lock: &lock{
			clientAddr: "null-client-addr",
		},
		secondaries: map[string]hbase.Type{},
	}
}

func (l *PrimaryLock) getPrimaryLock() ThemisLock {
	return l
}

func (l *PrimaryLock) addSecondaryColumn(col *hbase.ColumnCoordinate, t hbase.Type) {
	l.secondaries[col.String()] = t
}

func (l *PrimaryLock) toBytes() []byte {
	buf := bytes.NewBuffer(nil)
	// set is primary
	binary.Write(buf, binary.BigEndian, uint8(1))
	l.lock.write(buf)

	// write secondaries
	binary.Write(buf, binary.BigEndian, int32(len(l.secondaries)))
	for k, v := range l.secondaries {
		c := &hbase.ColumnCoordinate{}
		c.ParseFromString(k)
		c.Write(buf)
		buf.WriteByte(uint8(v))
	}
	return buf.Bytes()
}

func (l *PrimaryLock) isExpired() bool {
	return l.lock.expired
}

func (l *PrimaryLock) getSecondaryColumnType(c *hbase.ColumnCoordinate) hbase.Type {
	v, ok := l.secondaries[c.String()]
	if !ok {
		return hbase.TypeMinimum
	}
	return v
}

func (l *PrimaryLock) isPrimary() bool {
	return true
}

func (l *PrimaryLock) parseField(buf iohelper.ByteMultiReader) error {
	l.lock.parseField(buf)
	var sz int32
	err := binary.Read(buf, binary.BigEndian, &sz)
	if err != nil {
		return errors.Trace(err)
	}
	for i := 0; i < int(sz); i++ {
		c := &hbase.ColumnCoordinate{}
		c.ParseField(buf)
		b, err := buf.ReadByte()
		if err != nil {
			return errors.Trace(err)
		}
		t := hbase.Type(b)
		l.addSecondaryColumn(c, t)
	}
	return nil
}
