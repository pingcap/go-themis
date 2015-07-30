package themis

import (
	"bytes"
	"encoding/binary"
)

type PrimaryLock struct {
	*lock
	// {coordinate => type}
	secondaries map[string]Type
}

func newPrimaryLock() *PrimaryLock {
	return &PrimaryLock{
		lock: &lock{
			clientAddr: "null-client-addr",
		},
		secondaries: map[string]Type{},
	}
}

func (l *PrimaryLock) addSecondaryColumn(col *columnCoordinate, t Type) {
	l.secondaries[col.String()] = t
}

func (l *PrimaryLock) toBytes() []byte {
	buf := bytes.NewBuffer(nil)
	binary.Write(buf, binary.BigEndian, uint8(1))
	l.lock.write(buf)
	for k, v := range l.secondaries {
		c := &columnCoordinate{}
		c.parserFromString(k)
		c.write(buf)
		buf.WriteByte(uint8(v))
	}
	return buf.Bytes()
}

func (l *PrimaryLock) IsExpired() bool {
	return l.lock.expired
}

func (l *PrimaryLock) GetPrimaryLock() *PrimaryLock {
	return l
}

func (l *PrimaryLock) IsPrimary() bool {
	return true
}
