// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.35.1
// 	protoc        v3.12.4
// source: checkpoint.proto

package stormgrpc

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

type CheckpointRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Stage              int32  `protobuf:"varint,1,opt,name=stage,proto3" json:"stage,omitempty"`                                                       // stage: 1 , 2 , 3 (1 - transform, 2 - filter, 3 - aggregate)
	LineRangeProcessed int32  `protobuf:"varint,2,opt,name=line_range_processed,json=lineRangeProcessed,proto3" json:"line_range_processed,omitempty"` // stage 1 , 2
	Filename           string `protobuf:"bytes,3,opt,name=filename,proto3" json:"filename,omitempty"`                                                  // intermediate file name for the stage
	Vmname             string `protobuf:"bytes,4,opt,name=vmname,proto3" json:"vmname,omitempty"`                                                      // Name of the VM where task is running
	TaskId             int32  `protobuf:"varint,5,opt,name=task_id,json=taskId,proto3" json:"task_id,omitempty"`                                       // task sending the checkpoint
	Operation          string `protobuf:"bytes,6,opt,name=operation,proto3" json:"operation,omitempty"`                                                // operation: "src", "op1exe", "op2exe"
	State              string `protobuf:"bytes,7,opt,name=state,proto3" json:"state,omitempty"`                                                        //state of op
	Completed          bool   `protobuf:"varint,8,opt,name=completed,proto3" json:"completed,omitempty"`                                               // true if the task has completed the operation
}

func (x *CheckpointRequest) Reset() {
	*x = CheckpointRequest{}
	mi := &file_checkpoint_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *CheckpointRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CheckpointRequest) ProtoMessage() {}

func (x *CheckpointRequest) ProtoReflect() protoreflect.Message {
	mi := &file_checkpoint_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CheckpointRequest.ProtoReflect.Descriptor instead.
func (*CheckpointRequest) Descriptor() ([]byte, []int) {
	return file_checkpoint_proto_rawDescGZIP(), []int{0}
}

func (x *CheckpointRequest) GetStage() int32 {
	if x != nil {
		return x.Stage
	}
	return 0
}

func (x *CheckpointRequest) GetLineRangeProcessed() int32 {
	if x != nil {
		return x.LineRangeProcessed
	}
	return 0
}

func (x *CheckpointRequest) GetFilename() string {
	if x != nil {
		return x.Filename
	}
	return ""
}

func (x *CheckpointRequest) GetVmname() string {
	if x != nil {
		return x.Vmname
	}
	return ""
}

func (x *CheckpointRequest) GetTaskId() int32 {
	if x != nil {
		return x.TaskId
	}
	return 0
}

func (x *CheckpointRequest) GetOperation() string {
	if x != nil {
		return x.Operation
	}
	return ""
}

func (x *CheckpointRequest) GetState() string {
	if x != nil {
		return x.State
	}
	return ""
}

func (x *CheckpointRequest) GetCompleted() bool {
	if x != nil {
		return x.Completed
	}
	return false
}

type AckCheckpoint struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	LineAcked          int32 `protobuf:"varint,2,opt,name=line_acked,json=lineAcked,proto3" json:"line_acked,omitempty"`                              // line number saved in checkpoint ds at scheduler
	PrevStageCompleted bool  `protobuf:"varint,3,opt,name=prev_stage_completed,json=prevStageCompleted,proto3" json:"prev_stage_completed,omitempty"` // true if the previous stage has completed
}

func (x *AckCheckpoint) Reset() {
	*x = AckCheckpoint{}
	mi := &file_checkpoint_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *AckCheckpoint) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AckCheckpoint) ProtoMessage() {}

func (x *AckCheckpoint) ProtoReflect() protoreflect.Message {
	mi := &file_checkpoint_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AckCheckpoint.ProtoReflect.Descriptor instead.
func (*AckCheckpoint) Descriptor() ([]byte, []int) {
	return file_checkpoint_proto_rawDescGZIP(), []int{1}
}

func (x *AckCheckpoint) GetLineAcked() int32 {
	if x != nil {
		return x.LineAcked
	}
	return 0
}

func (x *AckCheckpoint) GetPrevStageCompleted() bool {
	if x != nil {
		return x.PrevStageCompleted
	}
	return false
}

var File_checkpoint_proto protoreflect.FileDescriptor

var file_checkpoint_proto_rawDesc = []byte{
	0x0a, 0x10, 0x63, 0x68, 0x65, 0x63, 0x6b, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x12, 0x09, 0x73, 0x74, 0x6f, 0x72, 0x6d, 0x67, 0x72, 0x70, 0x63, 0x22, 0xfa, 0x01,
	0x0a, 0x11, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x12, 0x14, 0x0a, 0x05, 0x73, 0x74, 0x61, 0x67, 0x65, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x05, 0x52, 0x05, 0x73, 0x74, 0x61, 0x67, 0x65, 0x12, 0x30, 0x0a, 0x14, 0x6c, 0x69, 0x6e,
	0x65, 0x5f, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x5f, 0x70, 0x72, 0x6f, 0x63, 0x65, 0x73, 0x73, 0x65,
	0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05, 0x52, 0x12, 0x6c, 0x69, 0x6e, 0x65, 0x52, 0x61, 0x6e,
	0x67, 0x65, 0x50, 0x72, 0x6f, 0x63, 0x65, 0x73, 0x73, 0x65, 0x64, 0x12, 0x1a, 0x0a, 0x08, 0x66,
	0x69, 0x6c, 0x65, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x66,
	0x69, 0x6c, 0x65, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x16, 0x0a, 0x06, 0x76, 0x6d, 0x6e, 0x61, 0x6d,
	0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x76, 0x6d, 0x6e, 0x61, 0x6d, 0x65, 0x12,
	0x17, 0x0a, 0x07, 0x74, 0x61, 0x73, 0x6b, 0x5f, 0x69, 0x64, 0x18, 0x05, 0x20, 0x01, 0x28, 0x05,
	0x52, 0x06, 0x74, 0x61, 0x73, 0x6b, 0x49, 0x64, 0x12, 0x1c, 0x0a, 0x09, 0x6f, 0x70, 0x65, 0x72,
	0x61, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x06, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x6f, 0x70, 0x65,
	0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x14, 0x0a, 0x05, 0x73, 0x74, 0x61, 0x74, 0x65, 0x18,
	0x07, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x73, 0x74, 0x61, 0x74, 0x65, 0x12, 0x1c, 0x0a, 0x09,
	0x63, 0x6f, 0x6d, 0x70, 0x6c, 0x65, 0x74, 0x65, 0x64, 0x18, 0x08, 0x20, 0x01, 0x28, 0x08, 0x52,
	0x09, 0x63, 0x6f, 0x6d, 0x70, 0x6c, 0x65, 0x74, 0x65, 0x64, 0x22, 0x60, 0x0a, 0x0d, 0x41, 0x63,
	0x6b, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x12, 0x1d, 0x0a, 0x0a, 0x6c,
	0x69, 0x6e, 0x65, 0x5f, 0x61, 0x63, 0x6b, 0x65, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05, 0x52,
	0x09, 0x6c, 0x69, 0x6e, 0x65, 0x41, 0x63, 0x6b, 0x65, 0x64, 0x12, 0x30, 0x0a, 0x14, 0x70, 0x72,
	0x65, 0x76, 0x5f, 0x73, 0x74, 0x61, 0x67, 0x65, 0x5f, 0x63, 0x6f, 0x6d, 0x70, 0x6c, 0x65, 0x74,
	0x65, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x08, 0x52, 0x12, 0x70, 0x72, 0x65, 0x76, 0x53, 0x74,
	0x61, 0x67, 0x65, 0x43, 0x6f, 0x6d, 0x70, 0x6c, 0x65, 0x74, 0x65, 0x64, 0x32, 0x59, 0x0a, 0x11,
	0x43, 0x68, 0x65, 0x63, 0x6b, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63,
	0x65, 0x12, 0x44, 0x0a, 0x0a, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x12,
	0x1c, 0x2e, 0x73, 0x74, 0x6f, 0x72, 0x6d, 0x67, 0x72, 0x70, 0x63, 0x2e, 0x43, 0x68, 0x65, 0x63,
	0x6b, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x18, 0x2e,
	0x73, 0x74, 0x6f, 0x72, 0x6d, 0x67, 0x72, 0x70, 0x63, 0x2e, 0x41, 0x63, 0x6b, 0x43, 0x68, 0x65,
	0x63, 0x6b, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x42, 0x0f, 0x5a, 0x0d, 0x2e, 0x2e, 0x2f, 0x73, 0x74,
	0x6f, 0x72, 0x6d, 0x67, 0x72, 0x70, 0x63, 0x2f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_checkpoint_proto_rawDescOnce sync.Once
	file_checkpoint_proto_rawDescData = file_checkpoint_proto_rawDesc
)

func file_checkpoint_proto_rawDescGZIP() []byte {
	file_checkpoint_proto_rawDescOnce.Do(func() {
		file_checkpoint_proto_rawDescData = protoimpl.X.CompressGZIP(file_checkpoint_proto_rawDescData)
	})
	return file_checkpoint_proto_rawDescData
}

var file_checkpoint_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_checkpoint_proto_goTypes = []any{
	(*CheckpointRequest)(nil), // 0: stormgrpc.CheckpointRequest
	(*AckCheckpoint)(nil),     // 1: stormgrpc.AckCheckpoint
}
var file_checkpoint_proto_depIdxs = []int32{
	0, // 0: stormgrpc.CheckpointService.Checkpoint:input_type -> stormgrpc.CheckpointRequest
	1, // 1: stormgrpc.CheckpointService.Checkpoint:output_type -> stormgrpc.AckCheckpoint
	1, // [1:2] is the sub-list for method output_type
	0, // [0:1] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_checkpoint_proto_init() }
func file_checkpoint_proto_init() {
	if File_checkpoint_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_checkpoint_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_checkpoint_proto_goTypes,
		DependencyIndexes: file_checkpoint_proto_depIdxs,
		MessageInfos:      file_checkpoint_proto_msgTypes,
	}.Build()
	File_checkpoint_proto = out.File
	file_checkpoint_proto_rawDesc = nil
	file_checkpoint_proto_goTypes = nil
	file_checkpoint_proto_depIdxs = nil
}
