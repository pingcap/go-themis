package themis

import (
	"bytes"
	"encoding/binary"
)

type SecondaryLock struct {
	*lock
	primaryCoordinate *columnCoordinate
}

func newSecondaryLock() *SecondaryLock {
	return &SecondaryLock{
		lock: &lock{
			clientAddr: "null-client-addr",
		},
		primaryCoordinate: &columnCoordinate{},
	}
}

func (l *SecondaryLock) IsExpired() bool {
	return l.lock.expired
}

func (l *SecondaryLock) GetPrimaryLock() *PrimaryLock {
	return &PrimaryLock{
		lock: l.lock,
	}
}

func (l *SecondaryLock) IsPrimary() bool {
	return false
}

func (l *SecondaryLock) toBytes() []byte {
	buf := bytes.NewBuffer(nil)
	binary.Write(buf, binary.BigEndian, uint8(0))
	l.lock.write(buf)
	l.primaryCoordinate.write(buf)
	return buf.Bytes()
}
