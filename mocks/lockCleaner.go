package mocks

import "github.com/pingcap/go-themis"
import "github.com/stretchr/testify/mock"

import "github.com/pingcap/go-themis/hbase"

type lockCleaner struct {
	mock.Mock
}

func (_m *lockCleaner) cleanPrimaryLock(cc *hbase.ColumnCoordinate, prewriteTs uint64) (uint64, themis.ThemisLock, error) {
	ret := _m.Called(cc, prewriteTs)

	var r0 uint64
	if rf, ok := ret.Get(0).(func(*hbase.ColumnCoordinate, uint64) uint64); ok {
		r0 = rf(cc, prewriteTs)
	} else {
		r0 = ret.Get(0).(uint64)
	}

	var r1 themis.ThemisLock
	if rf, ok := ret.Get(1).(func(*hbase.ColumnCoordinate, uint64) themis.ThemisLock); ok {
		r1 = rf(cc, prewriteTs)
	} else {
		r1 = ret.Get(1).(themis.ThemisLock)
	}

	var r2 error
	if rf, ok := ret.Get(2).(func(*hbase.ColumnCoordinate, uint64) error); ok {
		r2 = rf(cc, prewriteTs)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}
func (_m *lockCleaner) eraseLockAndData(tbl []byte, row []byte, col hbase.Column, ts uint64) error {
	ret := _m.Called(tbl, row, col, ts)

	var r0 error
	if rf, ok := ret.Get(0).(func([]byte, []byte, hbase.Column, uint64) error); ok {
		r0 = rf(tbl, row, col, ts)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
