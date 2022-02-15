package store

import (
	"encoding/binary"
	"io"
	"time"

	"github.com/zywaited/delay-queue/role/task"
)

const (
	FileStoreName        SType = "task-file-store"
	taskHeaderSize             = 14
	taskReallySize             = 8 * 1024
	taskBodySize               = taskReallySize - taskHeaderSize
	defaultFileCheckTime       = time.Minute
	defaultFileIdleTime        = time.Minute * 5
	defaultFileCheckNum        = 25
	maxMemoryFreeOffset        = 127
	maxChannelCap              = 128
)

type taskProto interface {
	Marshal(task.Task) ([]byte, error)
	UnMarshal([]byte) (task.Task, error)
}

type taskHeader struct {
	pos        uint16 // 可不用
	num        uint16 // 数量
	lastOffset uint16 // 写入的最后节点
	nextOffset int64  // 下一个节点
}

func (th *taskHeader) marshal() ([]byte, error) {
	bs := make([]byte, taskHeaderSize)
	binary.BigEndian.PutUint16(bs, th.pos)
	binary.BigEndian.PutUint16(bs[2:], th.num)
	binary.BigEndian.PutUint16(bs[4:], th.lastOffset)
	binary.BigEndian.PutUint64(bs[6:], uint64(th.nextOffset))
	return bs, nil
}

func (th *taskHeader) unmarshal(bs []byte) error {
	th.pos = binary.BigEndian.Uint16(bs)
	th.num = binary.BigEndian.Uint16(bs[2:])
	th.lastOffset = binary.BigEndian.Uint16(bs[4:])
	th.nextOffset = int64(binary.BigEndian.Uint64(bs[6:]))
	return nil
}

type fileAddTaskResult struct {
	err         error
	writeOffset int64
	lastOffset  int64
}

type fileReceiveResult struct {
	err        error
	nextOffset int64 // 用于重试
	task       []task.Task
}

type fileAddTask struct {
	pos         int
	writeOffset int64 // 中间流转的时候使用，初始化的时候应该默认-1
	lastOffset  int64
	task        []task.Task
	finish      chan fileAddTaskResult
}

type fileReceiveTask struct {
	pos    int
	offset int64
	finish chan fileReceiveResult
}

type fileOffset struct {
	writeOffset int64
	lastOffset  int64
}

type fileReadeWriteSeeker struct {
	now  time.Time
	file io.ReadWriteSeeker
}

type fileReaderNextOffset struct {
	file   io.ReadWriteSeeker
	finish chan int64
}

type fileWriterNextOffset struct {
	file   io.ReadWriteSeeker
	offset int64
	finish chan struct{}
}
