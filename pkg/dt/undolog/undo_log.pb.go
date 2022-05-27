//
// Copyright 2022 CECTC, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.27.1
// 	protoc        v3.11.4
// source: undo_log.proto

package undolog

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type PbSqlUndoLog struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	IsBinary    bool            `protobuf:"varint,1,opt,name=IsBinary,proto3" json:"IsBinary,omitempty"`
	SqlType     int32           `protobuf:"varint,2,opt,name=SqlType,proto3" json:"SqlType,omitempty"`
	SchemaName  string          `protobuf:"bytes,3,opt,name=SchemaName,proto3" json:"SchemaName,omitempty"`
	TableName   string          `protobuf:"bytes,4,opt,name=TableName,proto3" json:"TableName,omitempty"`
	LockKey     string          `protobuf:"bytes,5,opt,name=LockKey,proto3" json:"LockKey,omitempty"`
	BeforeImage *PbTableRecords `protobuf:"bytes,6,opt,name=BeforeImage,proto3" json:"BeforeImage,omitempty"`
	AfterImage  *PbTableRecords `protobuf:"bytes,7,opt,name=AfterImage,proto3" json:"AfterImage,omitempty"`
}

func (x *PbSqlUndoLog) Reset() {
	*x = PbSqlUndoLog{}
	if protoimpl.UnsafeEnabled {
		mi := &file_undo_log_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PbSqlUndoLog) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PbSqlUndoLog) ProtoMessage() {}

func (x *PbSqlUndoLog) ProtoReflect() protoreflect.Message {
	mi := &file_undo_log_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PbSqlUndoLog.ProtoReflect.Descriptor instead.
func (*PbSqlUndoLog) Descriptor() ([]byte, []int) {
	return file_undo_log_proto_rawDescGZIP(), []int{0}
}

func (x *PbSqlUndoLog) GetIsBinary() bool {
	if x != nil {
		return x.IsBinary
	}
	return false
}

func (x *PbSqlUndoLog) GetSqlType() int32 {
	if x != nil {
		return x.SqlType
	}
	return 0
}

func (x *PbSqlUndoLog) GetSchemaName() string {
	if x != nil {
		return x.SchemaName
	}
	return ""
}

func (x *PbSqlUndoLog) GetTableName() string {
	if x != nil {
		return x.TableName
	}
	return ""
}

func (x *PbSqlUndoLog) GetLockKey() string {
	if x != nil {
		return x.LockKey
	}
	return ""
}

func (x *PbSqlUndoLog) GetBeforeImage() *PbTableRecords {
	if x != nil {
		return x.BeforeImage
	}
	return nil
}

func (x *PbSqlUndoLog) GetAfterImage() *PbTableRecords {
	if x != nil {
		return x.AfterImage
	}
	return nil
}

type PbBranchUndoLog struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Xid         string          `protobuf:"bytes,1,opt,name=Xid,proto3" json:"Xid,omitempty"`
	BranchID    int64           `protobuf:"varint,2,opt,name=BranchID,proto3" json:"BranchID,omitempty"`
	SqlUndoLogs []*PbSqlUndoLog `protobuf:"bytes,3,rep,name=SqlUndoLogs,proto3" json:"SqlUndoLogs,omitempty"`
}

func (x *PbBranchUndoLog) Reset() {
	*x = PbBranchUndoLog{}
	if protoimpl.UnsafeEnabled {
		mi := &file_undo_log_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PbBranchUndoLog) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PbBranchUndoLog) ProtoMessage() {}

func (x *PbBranchUndoLog) ProtoReflect() protoreflect.Message {
	mi := &file_undo_log_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PbBranchUndoLog.ProtoReflect.Descriptor instead.
func (*PbBranchUndoLog) Descriptor() ([]byte, []int) {
	return file_undo_log_proto_rawDescGZIP(), []int{1}
}

func (x *PbBranchUndoLog) GetXid() string {
	if x != nil {
		return x.Xid
	}
	return ""
}

func (x *PbBranchUndoLog) GetBranchID() int64 {
	if x != nil {
		return x.BranchID
	}
	return 0
}

func (x *PbBranchUndoLog) GetSqlUndoLogs() []*PbSqlUndoLog {
	if x != nil {
		return x.SqlUndoLogs
	}
	return nil
}

var File_undo_log_proto protoreflect.FileDescriptor

var file_undo_log_proto_rawDesc = []byte{
	0x0a, 0x0e, 0x75, 0x6e, 0x64, 0x6f, 0x5f, 0x6c, 0x6f, 0x67, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x12, 0x07, 0x75, 0x6e, 0x64, 0x6f, 0x6c, 0x6f, 0x67, 0x1a, 0x13, 0x74, 0x61, 0x62, 0x6c, 0x65,
	0x5f, 0x72, 0x65, 0x63, 0x6f, 0x72, 0x64, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x90,
	0x02, 0x0a, 0x0c, 0x50, 0x62, 0x53, 0x71, 0x6c, 0x55, 0x6e, 0x64, 0x6f, 0x4c, 0x6f, 0x67, 0x12,
	0x1a, 0x0a, 0x08, 0x49, 0x73, 0x42, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x08, 0x52, 0x08, 0x49, 0x73, 0x42, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x12, 0x18, 0x0a, 0x07, 0x53,
	0x71, 0x6c, 0x54, 0x79, 0x70, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05, 0x52, 0x07, 0x53, 0x71,
	0x6c, 0x54, 0x79, 0x70, 0x65, 0x12, 0x1e, 0x0a, 0x0a, 0x53, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x4e,
	0x61, 0x6d, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0a, 0x53, 0x63, 0x68, 0x65, 0x6d,
	0x61, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x1c, 0x0a, 0x09, 0x54, 0x61, 0x62, 0x6c, 0x65, 0x4e, 0x61,
	0x6d, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x54, 0x61, 0x62, 0x6c, 0x65, 0x4e,
	0x61, 0x6d, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x4c, 0x6f, 0x63, 0x6b, 0x4b, 0x65, 0x79, 0x18, 0x05,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x4c, 0x6f, 0x63, 0x6b, 0x4b, 0x65, 0x79, 0x12, 0x39, 0x0a,
	0x0b, 0x42, 0x65, 0x66, 0x6f, 0x72, 0x65, 0x49, 0x6d, 0x61, 0x67, 0x65, 0x18, 0x06, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x17, 0x2e, 0x75, 0x6e, 0x64, 0x6f, 0x6c, 0x6f, 0x67, 0x2e, 0x50, 0x62, 0x54,
	0x61, 0x62, 0x6c, 0x65, 0x52, 0x65, 0x63, 0x6f, 0x72, 0x64, 0x73, 0x52, 0x0b, 0x42, 0x65, 0x66,
	0x6f, 0x72, 0x65, 0x49, 0x6d, 0x61, 0x67, 0x65, 0x12, 0x37, 0x0a, 0x0a, 0x41, 0x66, 0x74, 0x65,
	0x72, 0x49, 0x6d, 0x61, 0x67, 0x65, 0x18, 0x07, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x17, 0x2e, 0x75,
	0x6e, 0x64, 0x6f, 0x6c, 0x6f, 0x67, 0x2e, 0x50, 0x62, 0x54, 0x61, 0x62, 0x6c, 0x65, 0x52, 0x65,
	0x63, 0x6f, 0x72, 0x64, 0x73, 0x52, 0x0a, 0x41, 0x66, 0x74, 0x65, 0x72, 0x49, 0x6d, 0x61, 0x67,
	0x65, 0x22, 0x78, 0x0a, 0x0f, 0x50, 0x62, 0x42, 0x72, 0x61, 0x6e, 0x63, 0x68, 0x55, 0x6e, 0x64,
	0x6f, 0x4c, 0x6f, 0x67, 0x12, 0x10, 0x0a, 0x03, 0x58, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x03, 0x58, 0x69, 0x64, 0x12, 0x1a, 0x0a, 0x08, 0x42, 0x72, 0x61, 0x6e, 0x63, 0x68,
	0x49, 0x44, 0x18, 0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x08, 0x42, 0x72, 0x61, 0x6e, 0x63, 0x68,
	0x49, 0x44, 0x12, 0x37, 0x0a, 0x0b, 0x53, 0x71, 0x6c, 0x55, 0x6e, 0x64, 0x6f, 0x4c, 0x6f, 0x67,
	0x73, 0x18, 0x03, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x15, 0x2e, 0x75, 0x6e, 0x64, 0x6f, 0x6c, 0x6f,
	0x67, 0x2e, 0x50, 0x62, 0x53, 0x71, 0x6c, 0x55, 0x6e, 0x64, 0x6f, 0x4c, 0x6f, 0x67, 0x52, 0x0b,
	0x53, 0x71, 0x6c, 0x55, 0x6e, 0x64, 0x6f, 0x4c, 0x6f, 0x67, 0x73, 0x42, 0x0b, 0x5a, 0x09, 0x2e,
	0x3b, 0x75, 0x6e, 0x64, 0x6f, 0x6c, 0x6f, 0x67, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_undo_log_proto_rawDescOnce sync.Once
	file_undo_log_proto_rawDescData = file_undo_log_proto_rawDesc
)

func file_undo_log_proto_rawDescGZIP() []byte {
	file_undo_log_proto_rawDescOnce.Do(func() {
		file_undo_log_proto_rawDescData = protoimpl.X.CompressGZIP(file_undo_log_proto_rawDescData)
	})
	return file_undo_log_proto_rawDescData
}

var file_undo_log_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_undo_log_proto_goTypes = []interface{}{
	(*PbSqlUndoLog)(nil),    // 0: undolog.PbSqlUndoLog
	(*PbBranchUndoLog)(nil), // 1: undolog.PbBranchUndoLog
	(*PbTableRecords)(nil),  // 2: undolog.PbTableRecords
}
var file_undo_log_proto_depIdxs = []int32{
	2, // 0: undolog.PbSqlUndoLog.BeforeImage:type_name -> undolog.PbTableRecords
	2, // 1: undolog.PbSqlUndoLog.AfterImage:type_name -> undolog.PbTableRecords
	0, // 2: undolog.PbBranchUndoLog.SqlUndoLogs:type_name -> undolog.PbSqlUndoLog
	3, // [3:3] is the sub-list for method output_type
	3, // [3:3] is the sub-list for method input_type
	3, // [3:3] is the sub-list for extension type_name
	3, // [3:3] is the sub-list for extension extendee
	0, // [0:3] is the sub-list for field type_name
}

func init() { file_undo_log_proto_init() }
func file_undo_log_proto_init() {
	if File_undo_log_proto != nil {
		return
	}
	file_table_records_proto_init()
	if !protoimpl.UnsafeEnabled {
		file_undo_log_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PbSqlUndoLog); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_undo_log_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PbBranchUndoLog); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_undo_log_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_undo_log_proto_goTypes,
		DependencyIndexes: file_undo_log_proto_depIdxs,
		MessageInfos:      file_undo_log_proto_msgTypes,
	}.Build()
	File_undo_log_proto = out.File
	file_undo_log_proto_rawDesc = nil
	file_undo_log_proto_goTypes = nil
	file_undo_log_proto_depIdxs = nil
}
