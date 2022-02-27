package store

import (
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/zywaited/delay-queue/parser/system"
	"github.com/zywaited/delay-queue/role/task"
)

type file struct {
	// config
	dir          string
	maxMemoryNum int // 最大内存存储数量
	logger       system.Logger
	level        int // 第几层
	pos          int // 当前层的索引
	fp           *sync.Pool
	ip           *sync.Pool

	// store
	total          int // 当前存储数量
	memory         []task.Task
	fileReadWriter *fileExec
	fileWriteStart fileOffset
}

func (f *file) Add(t task.Task) error {
	if f.maxMemoryNum > 0 && len(f.memory) < f.maxMemoryNum {
		f.memory = append(f.memory, t)
		f.total++
		return nil
	}
	tasks := make([]task.Task, 0, 1+f.maxMemoryNum>>1)
	if f.maxMemoryNum > 0 {
		// 截取一半
		half := len(f.memory) - (f.maxMemoryNum >> 1) - 1 // >= 0
		tasks = append(tasks, f.memory[half:]...)
		f.memory = f.memory[:half]
	}
	tasks = append(tasks, t)
	ft := &fileAddTask{
		pos: f.pos, lastOffset: f.fileWriteStart.lastOffset,
		task:   tasks,
		finish: make(chan fileAddTaskResult, 1), // cap > 0 用于异常情况
	}
	f.fileReadWriter.addChannel <- ft
	fr := <-ft.finish
	if fr.err == nil {
		if f.fileWriteStart.writeOffset == -1 {
			f.fileWriteStart.writeOffset = fr.writeOffset
		}
		f.fileWriteStart.lastOffset = fr.lastOffset
		f.total++
		return nil
	}
	if f.maxMemoryNum <= 0 {
		return errors.WithMessage(fr.err, "file store add failed")
	}
	if f.logger != nil {
		f.logger.Infof("[%d-%d]file store add failed: %s", f.level, f.pos, fr.err.Error())
	}
	// recover memory
	f.memory = append(f.memory, tasks...)
	f.total++
	return nil
}

func (f *file) Remove(_ string) error {
	// nothing
	return nil
}

func (f *file) Len() int {
	return f.total
}

func (f *file) Get(_ string) task.Task {
	// nothing
	return nil
}

func (f *file) Release() {
	fp := f.fp
	f.fp = nil
	f.ip = nil
	f.memory = f.memory[:0]
	f.fileReadWriter = nil
	f.logger = nil
	fp.Put(f)
}

func (f *file) Iter() task.StoreIterator {
	fi := f.ip.Get().(*fileIterator)
	fi.fc = f
	fi.Rewind()
	return fi
}

type fileIterator struct {
	fc *file

	readNum       int
	memoryIndex   int
	fileIndex     int
	fileReadStart int64
	fileTasks     []task.Task
}

func (f *fileIterator) Len() int {
	return f.fc.total
}

func (f *fileIterator) Next() bool {
	return f.readNum < f.fc.total
}

func (f *fileIterator) Rewind() {
	f.memoryIndex = 0
	f.fileIndex = 0
	f.fileReadStart = f.fc.fileWriteStart.writeOffset
	f.readNum = 0
}

func (f *fileIterator) Scan() task.Task {
	f.readNum++
	if f.memoryIndex < len(f.fc.memory) {
		t := f.fc.memory[f.memoryIndex]
		f.memoryIndex++
		return t
	}
	if f.fileIndex == len(f.fileTasks) {
	retryRead:
		ft := &fileReceiveTask{
			pos:    f.fc.pos,
			offset: f.fileReadStart,
			finish: make(chan fileReceiveResult, 1),
		}
		f.fc.fileReadWriter.receiveChannel <- ft
		fr := <-ft.finish
		if fr.err != nil {
			if f.fc.logger != nil {
				f.fc.logger.Infof("[%d-%d]file store read failed: %s", f.fc.level, f.fc.pos, fr.err.Error())
			}
			// note: just sleep 1ms
			time.Sleep(time.Millisecond)
			// todo 报警机制【理论上除了负载和脏数据不会出现】
			goto retryRead
		}
		f.fileTasks = fr.task
		f.fileReadStart = fr.nextOffset
		f.fileIndex = 0
	}
	t := f.fileTasks[f.fileIndex]
	f.fileIndex++
	return t
}

func (f *fileIterator) Release() {
	f.fileTasks = nil
	ip := f.fc.ip
	f.fc = nil
	ip.Put(f)
}

type FilePoolOption func(*filePool)

func FilePoolOptionWithLogger(logger system.Logger) FilePoolOption {
	return func(fp *filePool) {
		fp.logger = logger
	}
}

func FilePoolOptionWithTaskProto(proto taskProto) FilePoolOption {
	return func(fp *filePool) {
		fp.proto = proto
	}
}

type FileConfig struct {
	Dir            string
	MaxMemoryNum   int // 最大内存存储数量
	LevelMaxReader int
	LevelMaxWriter int
}

const (
	defaultMinMemoryNum = 100
	defaultReduceFactor = 0.15
)

type filePool struct {
	c      *FileConfig
	logger system.Logger
	proto  taskProto
	fm     map[int]*fileExec
	locker sync.RWMutex
	fp     sync.Pool
	ip     sync.Pool
}

func (fp *filePool) NewTaskStore(level, pos int) task.Store {
	fp.locker.RLock()
	fe, exist := fp.fm[level]
	fp.locker.RUnlock()
	if !exist {
		fp.locker.Lock()
		fe, exist = fp.fm[level]
		if !exist {
			fe = newFileExec(
				fileExecOptionWithDir(fp.c.Dir, level),
				fileExecOptionWithMaxFile(fp.c.LevelMaxReader, fp.c.LevelMaxWriter),
				fileExecOptionWithProto(fp.proto),
				fileExecOptionWithLogger(fp.logger),
			)
			go fe.run()
			fp.fm[level] = fe
		}
		fp.locker.Unlock()
	}
	f := fp.fp.Get().(*file)
	// init
	// config
	f.dir = fp.c.Dir
	f.maxMemoryNum = fp.c.MaxMemoryNum * (1 - int(float64(level-1)*defaultReduceFactor))
	if f.maxMemoryNum < defaultMinMemoryNum {
		f.maxMemoryNum = defaultMinMemoryNum
	}
	f.level = level
	f.pos = pos
	f.logger = fp.logger

	// instance
	f.fp = &fp.fp
	f.ip = &fp.ip
	f.total = 0
	f.memory = f.memory[:0]
	f.fileReadWriter = fe
	f.fileWriteStart.writeOffset = -1
	f.fileWriteStart.lastOffset = -1
	return f
}

func NewFilePool(c *FileConfig, opts ...FilePoolOption) *filePool {
	fp := filePool{c: c, fm: make(map[int]*fileExec)}
	for _, opt := range opts {
		opt(&fp)
	}
	fp.fp.New = func() interface{} {
		return &file{}
	}
	fp.ip.New = func() interface{} {
		return &fileIterator{}
	}
	return &fp
}
