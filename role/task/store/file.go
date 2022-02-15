package store

import (
	"github.com/zywaited/delay-queue/parser/system"
	"github.com/zywaited/delay-queue/role/task"
)

// todo
// 1: task pool's pos && level init && interface
// 2: InitTaskUnmarshal interface init
// 3: task unmarshal && marshal handler
// 4: file store ??

type file struct {
	dir          string
	maxMemoryNum int // 最大内存存储数量
	logger       system.Logger
	level        int // 第几层
	pos          int // 当前层的索引

	total          int        // 当前存储数量
	memory         task.Store // taskListPool.NewTaskStore
	fileReadWriter *fileExec
	fileWriteStart fileOffset
}

func (f *file) Add(task.Task) error {
	panic("implement me")
}

func (f *file) Remove(string) error {
	panic("implement me")
}

func (f *file) Len() int {
	panic("implement me")
}

func (f *file) Get(string) task.Task {
	panic("implement me")
}

func (f *file) Release() {
	panic("implement me")
}

func (f *file) Iter() task.StoreIterator {
	panic("implement me")
}

type FilePoolOption func(*filePool)

func FilePoolOptionWithDir(dir string) FilePoolOption {
	return func(fp *filePool) {
		fp.dir = dir
	}
}

func FilePoolOptionWithLogger(logger system.Logger) FilePoolOption {
	return func(fp *filePool) {
		fp.logger = logger
	}
}

func FilePoolOptionWithLevelConfigs(fcs ...FileLevelConfig) FilePoolOption {
	return func(fp *filePool) {
		for _, c := range fcs {
			fp.lm[c.Level] = c
		}
	}
}

func FilePoolOptionWithTaskProto(proto taskProto) FilePoolOption {
	return func(fp *filePool) {
		fp.proto = proto
	}
}

type FileLevelConfig struct {
	MaxNum         int // 最大文件存储数量
	MaxMemoryNum   int // 最大内存存储数量
	Level          int // 第几层
	LevelMaxReader int
}

type filePool struct {
	dir    string
	logger system.Logger
	proto  taskProto
	lm     map[int]FileLevelConfig
}

func (fp *filePool) NewTaskStore(level, pos int) task.Store {
	_ = fp.lm[level]
	// todo
	return nil
}

func NewFilePool(opts ...FilePoolOption) *filePool {
	fp := filePool{}
	for _, opt := range opts {
		opt(&fp)
	}
	return &fp
}
