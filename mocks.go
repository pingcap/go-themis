package themis

import "github.com/stretchr/testify/mock"

import "github.com/pingcap/go-themis/hbase"

type mockLockCleaner struct {
	mock.Mock
}

func (_m *mockLockCleaner) cleanPrimaryLock(cc *hbase.ColumnCoordinate, prewriteTs uint64) (uint64, ThemisLock, error) {
	ret := _m.Called(cc, prewriteTs)

	var r0 uint64
	if rf, ok := ret.Get(0).(func(*hbase.ColumnCoordinate, uint64) uint64); ok {
		r0 = rf(cc, prewriteTs)
	} else {
		r0 = ret.Get(0).(uint64)
	}

	var r1 ThemisLock
	if rf, ok := ret.Get(1).(func(*hbase.ColumnCoordinate, uint64) ThemisLock); ok {
		r1 = rf(cc, prewriteTs)
	} else {
		r1 = ret.Get(1).(ThemisLock)
	}

	var r2 error
	if rf, ok := ret.Get(2).(func(*hbase.ColumnCoordinate, uint64) error); ok {
		r2 = rf(cc, prewriteTs)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}
func (_m *mockLockCleaner) eraseLockAndData(tbl []byte, row []byte, col hbase.Column, ts uint64) error {
	ret := _m.Called(tbl, row, col, ts)

	var r0 error
	if rf, ok := ret.Get(0).(func([]byte, []byte, hbase.Column, uint64) error); ok {
		r0 = rf(tbl, row, col, ts)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

type mockThemisClient struct {
	mock.Mock
}

func (_m *mockThemisClient) checkAndSetLockIsExpired(l ThemisLock, TTL uint64) (bool, error) {
	ret := _m.Called(l, TTL)

	var r0 bool
	if rf, ok := ret.Get(0).(func(ThemisLock, uint64) bool); ok {
		r0 = rf(l, TTL)
	} else {
		r0 = ret.Get(0).(bool)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(ThemisLock, uint64) error); ok {
		r1 = rf(l, TTL)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
func (_m *mockThemisClient) themisGet(tbl []byte, g *hbase.Get, startTs uint64) (*hbase.ResultRow, error) {
	ret := _m.Called(tbl, g, startTs)

	var r0 *hbase.ResultRow
	if rf, ok := ret.Get(0).(func([]byte, *hbase.Get, uint64) *hbase.ResultRow); ok {
		r0 = rf(tbl, g, startTs)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*hbase.ResultRow)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func([]byte, *hbase.Get, uint64) error); ok {
		r1 = rf(tbl, g, startTs)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
func (_m *mockThemisClient) prewriteRow(tbl []byte, row []byte, mutations []*columnMutation, prewriteTs uint64, primaryLockBytes []byte, secondaryLockBytes []byte, primaryOffset int) (ThemisLock, error) {
	ret := _m.Called(tbl, row, mutations, prewriteTs, primaryLockBytes, secondaryLockBytes, primaryOffset)

	var r0 ThemisLock
	if rf, ok := ret.Get(0).(func([]byte, []byte, []*columnMutation, uint64, []byte, []byte, int) ThemisLock); ok {
		r0 = rf(tbl, row, mutations, prewriteTs, primaryLockBytes, secondaryLockBytes, primaryOffset)
	} else {
		r0 = ret.Get(0).(ThemisLock)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func([]byte, []byte, []*columnMutation, uint64, []byte, []byte, int) error); ok {
		r1 = rf(tbl, row, mutations, prewriteTs, primaryLockBytes, secondaryLockBytes, primaryOffset)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
func (_m *mockThemisClient) isLockExpired(tbl []byte, row []byte, ts uint64) (bool, error) {
	ret := _m.Called(tbl, row, ts)

	var r0 bool
	if rf, ok := ret.Get(0).(func([]byte, []byte, uint64) bool); ok {
		r0 = rf(tbl, row, ts)
	} else {
		r0 = ret.Get(0).(bool)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func([]byte, []byte, uint64) error); ok {
		r1 = rf(tbl, row, ts)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
func (_m *mockThemisClient) getLockAndErase(cc *hbase.ColumnCoordinate, prewriteTs uint64) (ThemisLock, error) {
	ret := _m.Called(cc, prewriteTs)

	var r0 ThemisLock
	if rf, ok := ret.Get(0).(func(*hbase.ColumnCoordinate, uint64) ThemisLock); ok {
		r0 = rf(cc, prewriteTs)
	} else {
		r0 = ret.Get(0).(ThemisLock)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*hbase.ColumnCoordinate, uint64) error); ok {
		r1 = rf(cc, prewriteTs)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
func (_m *mockThemisClient) commitRow(tbl []byte, row []byte, mutations []*columnMutation, prewriteTs uint64, commitTs uint64, primaryOffset int) error {
	ret := _m.Called(tbl, row, mutations, prewriteTs, commitTs, primaryOffset)

	var r0 error
	if rf, ok := ret.Get(0).(func([]byte, []byte, []*columnMutation, uint64, uint64, int) error); ok {
		r0 = rf(tbl, row, mutations, prewriteTs, commitTs, primaryOffset)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
func (_m *mockThemisClient) commitSecondaryRow(tbl []byte, row []byte, mutations []*columnMutation, prewriteTs uint64, commitTs uint64) error {
	ret := _m.Called(tbl, row, mutations, prewriteTs, commitTs)

	var r0 error
	if rf, ok := ret.Get(0).(func([]byte, []byte, []*columnMutation, uint64, uint64) error); ok {
		r0 = rf(tbl, row, mutations, prewriteTs, commitTs)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
func (_m *mockThemisClient) prewriteSecondaryRow(tbl []byte, row []byte, mutations []*columnMutation, prewriteTs uint64, secondaryLockBytes []byte) (ThemisLock, error) {
	ret := _m.Called(tbl, row, mutations, prewriteTs, secondaryLockBytes)

	var r0 ThemisLock
	if rf, ok := ret.Get(0).(func([]byte, []byte, []*columnMutation, uint64, []byte) ThemisLock); ok {
		r0 = rf(tbl, row, mutations, prewriteTs, secondaryLockBytes)
	} else {
		r0 = ret.Get(0).(ThemisLock)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func([]byte, []byte, []*columnMutation, uint64, []byte) error); ok {
		r1 = rf(tbl, row, mutations, prewriteTs, secondaryLockBytes)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
