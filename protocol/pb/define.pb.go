// Code generated by protoc-gen-go. DO NOT EDIT.
// source: define.proto

package pb

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

// 根据文档定义常量
type TaskType int32

const (
	TaskType_Ignore         TaskType = 0
	TaskType_TaskDelay      TaskType = 1
	TaskType_TaskReady      TaskType = 2
	TaskType_TaskReserved   TaskType = 3
	TaskType_TaskFinished   TaskType = 4
	TaskType_TaskRetryDelay TaskType = 5
	TaskType_TaskCheck      TaskType = 6
)

var TaskType_name = map[int32]string{
	0: "Ignore",
	1: "TaskDelay",
	2: "TaskReady",
	3: "TaskReserved",
	4: "TaskFinished",
	5: "TaskRetryDelay",
	6: "TaskCheck",
}

var TaskType_value = map[string]int32{
	"Ignore":         0,
	"TaskDelay":      1,
	"TaskReady":      2,
	"TaskReserved":   3,
	"TaskFinished":   4,
	"TaskRetryDelay": 5,
	"TaskCheck":      6,
}

func (x TaskType) String() string {
	return proto.EnumName(TaskType_name, int32(x))
}

func (TaskType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_f7e38f9743a0f071, []int{0}
}

func init() {
	proto.RegisterEnum("pb.TaskType", TaskType_name, TaskType_value)
}

func init() { proto.RegisterFile("define.proto", fileDescriptor_f7e38f9743a0f071) }

var fileDescriptor_f7e38f9743a0f071 = []byte{
	// 140 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xe2, 0x49, 0x49, 0x4d, 0xcb,
	0xcc, 0x4b, 0xd5, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0x62, 0x2a, 0x48, 0xd2, 0xaa, 0xe6, 0xe2,
	0x08, 0x49, 0x2c, 0xce, 0x0e, 0xa9, 0x2c, 0x48, 0x15, 0xe2, 0xe2, 0x62, 0xf3, 0x4c, 0xcf, 0xcb,
	0x2f, 0x4a, 0x15, 0x60, 0x10, 0xe2, 0xe5, 0xe2, 0x04, 0x89, 0xbb, 0xa4, 0xe6, 0x24, 0x56, 0x0a,
	0x30, 0xc2, 0xb8, 0x41, 0xa9, 0x89, 0x29, 0x95, 0x02, 0x4c, 0x42, 0x02, 0x5c, 0x3c, 0x10, 0x6e,
	0x71, 0x6a, 0x51, 0x59, 0x6a, 0x8a, 0x00, 0x33, 0x4c, 0xc4, 0x2d, 0x33, 0x2f, 0xb3, 0x38, 0x23,
	0x35, 0x45, 0x80, 0x45, 0x48, 0x88, 0x8b, 0x0f, 0xa2, 0xa6, 0xa4, 0xa8, 0x12, 0x62, 0x0c, 0x2b,
	0xcc, 0x18, 0xe7, 0x8c, 0xd4, 0xe4, 0x6c, 0x01, 0xb6, 0x24, 0x36, 0xb0, 0x3b, 0x8c, 0x01, 0x01,
	0x00, 0x00, 0xff, 0xff, 0xcd, 0xaa, 0x13, 0x3e, 0x97, 0x00, 0x00, 0x00,
}
