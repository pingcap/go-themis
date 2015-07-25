// Code generated by protoc-gen-go.
// source: Themis.proto
// DO NOT EDIT!

package proto

import proto1 "github.com/golang/protobuf/proto"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto1.Marshal
var _ = math.Inf

type ThemisGetRequest struct {
	Get              *Get    `protobuf:"bytes,1,req,name=get" json:"get,omitempty"`
	StartTs          *uint64 `protobuf:"varint,2,req,name=startTs" json:"startTs,omitempty"`
	IgnoreLock       *bool   `protobuf:"varint,3,req,name=ignoreLock" json:"ignoreLock,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *ThemisGetRequest) Reset()         { *m = ThemisGetRequest{} }
func (m *ThemisGetRequest) String() string { return proto1.CompactTextString(m) }
func (*ThemisGetRequest) ProtoMessage()    {}

func (m *ThemisGetRequest) GetGet() *Get {
	if m != nil {
		return m.Get
	}
	return nil
}

func (m *ThemisGetRequest) GetStartTs() uint64 {
	if m != nil && m.StartTs != nil {
		return *m.StartTs
	}
	return 0
}

func (m *ThemisGetRequest) GetIgnoreLock() bool {
	if m != nil && m.IgnoreLock != nil {
		return *m.IgnoreLock
	}
	return false
}

type ThemisPrewriteRequest struct {
	Row              []byte  `protobuf:"bytes,1,req,name=row" json:"row,omitempty"`
	Mutations        []*Cell `protobuf:"bytes,2,rep,name=mutations" json:"mutations,omitempty"`
	PrewriteTs       *uint64 `protobuf:"varint,3,req,name=prewriteTs" json:"prewriteTs,omitempty"`
	SecondaryLock    []byte  `protobuf:"bytes,4,req,name=secondaryLock" json:"secondaryLock,omitempty"`
	PrimaryLock      []byte  `protobuf:"bytes,5,req,name=primaryLock" json:"primaryLock,omitempty"`
	PrimaryIndex     *int32  `protobuf:"varint,6,req,name=primaryIndex" json:"primaryIndex,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *ThemisPrewriteRequest) Reset()         { *m = ThemisPrewriteRequest{} }
func (m *ThemisPrewriteRequest) String() string { return proto1.CompactTextString(m) }
func (*ThemisPrewriteRequest) ProtoMessage()    {}

func (m *ThemisPrewriteRequest) GetRow() []byte {
	if m != nil {
		return m.Row
	}
	return nil
}

func (m *ThemisPrewriteRequest) GetMutations() []*Cell {
	if m != nil {
		return m.Mutations
	}
	return nil
}

func (m *ThemisPrewriteRequest) GetPrewriteTs() uint64 {
	if m != nil && m.PrewriteTs != nil {
		return *m.PrewriteTs
	}
	return 0
}

func (m *ThemisPrewriteRequest) GetSecondaryLock() []byte {
	if m != nil {
		return m.SecondaryLock
	}
	return nil
}

func (m *ThemisPrewriteRequest) GetPrimaryLock() []byte {
	if m != nil {
		return m.PrimaryLock
	}
	return nil
}

func (m *ThemisPrewriteRequest) GetPrimaryIndex() int32 {
	if m != nil && m.PrimaryIndex != nil {
		return *m.PrimaryIndex
	}
	return 0
}

type ThemisPrewriteResponse struct {
	Result           [][]byte `protobuf:"bytes,1,rep,name=result" json:"result,omitempty"`
	XXX_unrecognized []byte   `json:"-"`
}

func (m *ThemisPrewriteResponse) Reset()         { *m = ThemisPrewriteResponse{} }
func (m *ThemisPrewriteResponse) String() string { return proto1.CompactTextString(m) }
func (*ThemisPrewriteResponse) ProtoMessage()    {}

func (m *ThemisPrewriteResponse) GetResult() [][]byte {
	if m != nil {
		return m.Result
	}
	return nil
}

type ThemisCommitRequest struct {
	Row              []byte  `protobuf:"bytes,1,req,name=row" json:"row,omitempty"`
	Mutations        []*Cell `protobuf:"bytes,2,rep,name=mutations" json:"mutations,omitempty"`
	PrewriteTs       *uint64 `protobuf:"varint,3,req,name=prewriteTs" json:"prewriteTs,omitempty"`
	CommitTs         *uint64 `protobuf:"varint,4,req,name=commitTs" json:"commitTs,omitempty"`
	PrimaryIndex     *int32  `protobuf:"varint,5,req,name=primaryIndex" json:"primaryIndex,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *ThemisCommitRequest) Reset()         { *m = ThemisCommitRequest{} }
func (m *ThemisCommitRequest) String() string { return proto1.CompactTextString(m) }
func (*ThemisCommitRequest) ProtoMessage()    {}

func (m *ThemisCommitRequest) GetRow() []byte {
	if m != nil {
		return m.Row
	}
	return nil
}

func (m *ThemisCommitRequest) GetMutations() []*Cell {
	if m != nil {
		return m.Mutations
	}
	return nil
}

func (m *ThemisCommitRequest) GetPrewriteTs() uint64 {
	if m != nil && m.PrewriteTs != nil {
		return *m.PrewriteTs
	}
	return 0
}

func (m *ThemisCommitRequest) GetCommitTs() uint64 {
	if m != nil && m.CommitTs != nil {
		return *m.CommitTs
	}
	return 0
}

func (m *ThemisCommitRequest) GetPrimaryIndex() int32 {
	if m != nil && m.PrimaryIndex != nil {
		return *m.PrimaryIndex
	}
	return 0
}

type ThemisCommitResponse struct {
	Result           *bool  `protobuf:"varint,1,req,name=result" json:"result,omitempty"`
	XXX_unrecognized []byte `json:"-"`
}

func (m *ThemisCommitResponse) Reset()         { *m = ThemisCommitResponse{} }
func (m *ThemisCommitResponse) String() string { return proto1.CompactTextString(m) }
func (*ThemisCommitResponse) ProtoMessage()    {}

func (m *ThemisCommitResponse) GetResult() bool {
	if m != nil && m.Result != nil {
		return *m.Result
	}
	return false
}

type EraseLockRequest struct {
	Row              []byte  `protobuf:"bytes,1,req,name=row" json:"row,omitempty"`
	Family           []byte  `protobuf:"bytes,2,req,name=family" json:"family,omitempty"`
	Qualifier        []byte  `protobuf:"bytes,3,req,name=qualifier" json:"qualifier,omitempty"`
	PrewriteTs       *uint64 `protobuf:"varint,4,req,name=prewriteTs" json:"prewriteTs,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *EraseLockRequest) Reset()         { *m = EraseLockRequest{} }
func (m *EraseLockRequest) String() string { return proto1.CompactTextString(m) }
func (*EraseLockRequest) ProtoMessage()    {}

func (m *EraseLockRequest) GetRow() []byte {
	if m != nil {
		return m.Row
	}
	return nil
}

func (m *EraseLockRequest) GetFamily() []byte {
	if m != nil {
		return m.Family
	}
	return nil
}

func (m *EraseLockRequest) GetQualifier() []byte {
	if m != nil {
		return m.Qualifier
	}
	return nil
}

func (m *EraseLockRequest) GetPrewriteTs() uint64 {
	if m != nil && m.PrewriteTs != nil {
		return *m.PrewriteTs
	}
	return 0
}

type EraseLockResponse struct {
	Lock             []byte `protobuf:"bytes,1,opt,name=lock" json:"lock,omitempty"`
	XXX_unrecognized []byte `json:"-"`
}

func (m *EraseLockResponse) Reset()         { *m = EraseLockResponse{} }
func (m *EraseLockResponse) String() string { return proto1.CompactTextString(m) }
func (*EraseLockResponse) ProtoMessage()    {}

func (m *EraseLockResponse) GetLock() []byte {
	if m != nil {
		return m.Lock
	}
	return nil
}

type LockExpiredRequest struct {
	Timestamp        *uint64 `protobuf:"varint,1,req,name=timestamp" json:"timestamp,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *LockExpiredRequest) Reset()         { *m = LockExpiredRequest{} }
func (m *LockExpiredRequest) String() string { return proto1.CompactTextString(m) }
func (*LockExpiredRequest) ProtoMessage()    {}

func (m *LockExpiredRequest) GetTimestamp() uint64 {
	if m != nil && m.Timestamp != nil {
		return *m.Timestamp
	}
	return 0
}

type LockExpiredResponse struct {
	Expired          *bool  `protobuf:"varint,1,req,name=expired" json:"expired,omitempty"`
	XXX_unrecognized []byte `json:"-"`
}

func (m *LockExpiredResponse) Reset()         { *m = LockExpiredResponse{} }
func (m *LockExpiredResponse) String() string { return proto1.CompactTextString(m) }
func (*LockExpiredResponse) ProtoMessage()    {}

func (m *LockExpiredResponse) GetExpired() bool {
	if m != nil && m.Expired != nil {
		return *m.Expired
	}
	return false
}
