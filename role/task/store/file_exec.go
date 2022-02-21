package store

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/zywaited/delay-queue/parser/system"
	"github.com/zywaited/delay-queue/role/task"
)

var (
	notEnoughReader = errors.New("not enough reader")
	notEnoughWriter = errors.New("not enough writer")
)

type fileExec struct {
	// configs
	dir       string
	level     int // 第几层
	maxReader int
	maxWriter int
	logger    system.Logger
	proto     taskProto

	// internal
	reader            []fileReadeWriteSeeker
	currReader        int
	readWriter        []fileReadeWriteSeeker
	currWriter        int
	addChannel        chan *fileAddTask
	receiveChannel    chan *fileReceiveTask
	stopChannel       chan struct{}
	readerReady       chan io.ReadWriteSeeker
	writerReady       chan io.ReadWriteSeeker
	freeChannel       chan fileWriterNextOffset
	freeReady         chan struct{}
	nextOffsetChannel chan fileReaderNextOffset
	nextOffsetReady   chan struct{}
	writeNum          int64
	freeOffset        fileOffset
	memoryFreeOffsets []int64
}

func (fe *fileExec) _readTaskHeader(reader io.ReadWriteSeeker, offset int64) (*taskHeader, error) {
	_, err := reader.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, errors.WithMessage(err, "_readTaskHeader seek task-header failed")
	}
	hb := make([]byte, taskHeaderSize)
	hn, err := reader.Read(hb)
	if err != nil {
		return nil, errors.WithMessage(err, "_readTaskHeader read task-header failed")
	}
	if hn != len(hb) {
		return nil, errors.New("_readTaskHeader read task-header-size failed")
	}
	header := &taskHeader{}
	err = header.unmarshal(hb)
	if err != nil {
		return nil, errors.WithMessage(err, "_readTaskHeader unmarshal task-header failed")
	}
	return header, nil
}

func (fe *fileExec) _writeTaskHeader(writer io.WriteSeeker, offset int64, header *taskHeader) error {
	_, err := writer.Seek(offset, io.SeekStart)
	if err != nil {
		return errors.WithMessagef(err, "_writeTaskHeader seek task-header failed")
	}
	hb, _ := header.marshal()
	_, err = writer.Write(hb)
	if err != nil {
		return errors.WithMessagef(err, "_writeTaskHeader write task-header failed")
	}
	return nil
}

func (fe *fileExec) updateFreeOffset(readWriter io.ReadWriteSeeker, offset int64, finish chan struct{}) {
	var err error
	defer func() {
		rerr := recover()
		if rerr != nil && fe.logger != nil {
			fe.logger.Errorf("[%d]update free offset failed: %v, stack: %s", fe.level, rerr, system.Stack())
		}
		if err != nil {
			// note: 防止数据异常
			fe.memoryFreeOffsets = append(fe.memoryFreeOffsets, offset)
			if fe.logger != nil {
				fe.logger.Errorf("[%d]update free offset failed: %s", fe.level, err.Error())
			}
		}
		fe.freeReady <- struct{}{}
		finish <- struct{}{}
	}()
	if len(fe.memoryFreeOffsets) < maxMemoryFreeOffset {
		fe.memoryFreeOffsets = append(fe.memoryFreeOffsets, offset)
		return
	}
	// first offset on memory
	if fe.freeOffset.writeOffset == -1 || fe.freeOffset.lastOffset == -1 {
		fe.freeOffset.writeOffset = offset
		fe.freeOffset.lastOffset = offset
		return
	}
	// reset task header
	err = fe._writeTaskHeader(readWriter, offset, &taskHeader{nextOffset: -1})
	if err != nil {
		return
	}
	// update nextOffset
	header, err := fe._readTaskHeader(readWriter, fe.freeOffset.lastOffset)
	if err != nil {
		return
	}
	header.nextOffset = offset
	err = fe._writeTaskHeader(readWriter, fe.freeOffset.lastOffset, header)
	if err != nil {
		return
	}
	fe.freeOffset.lastOffset = offset
}

func (fe *fileExec) fetchNextOffset(reader io.ReadWriteSeeker, offsetChannel chan int64) {
	var (
		offset int64
		err    error
	)
	defer func() {
		rerr := recover()
		if rerr != nil && fe.logger != nil {
			fe.logger.Errorf("[%d]fetch next offset failed: %v, stack: %s", fe.level, rerr, system.Stack())
		}
		if err != nil {
			offset = -1
			if fe.logger != nil {
				fe.logger.Errorf("[%d]fetch next offset failed: %s", fe.level, err.Error())
			}
		}
		fe.nextOffsetReady <- struct{}{}
		offsetChannel <- offset
	}()
	// new a free offset
	if fe.freeOffset.writeOffset < 0 && len(fe.memoryFreeOffsets) == 0 {
		offset, err = reader.Seek(taskReallySize*fe.writeNum, io.SeekStart)
		if err == nil {
			fe.writeNum++
		}
		return
	}
	// memory free offset
	if len(fe.memoryFreeOffsets) > 0 {
		offset = fe.memoryFreeOffsets[len(fe.memoryFreeOffsets)-1]
		fe.memoryFreeOffsets = fe.memoryFreeOffsets[:len(fe.memoryFreeOffsets)-1]
		return
	}
	offset = fe.freeOffset.writeOffset
	// only one free offset
	if fe.freeOffset.writeOffset == fe.freeOffset.lastOffset {
		fe.freeOffset.writeOffset = -1
		fe.freeOffset.lastOffset = -1
		return
	}
	header, err := fe._readTaskHeader(reader, offset)
	if err != nil {
		return
	}
	fe.freeOffset.writeOffset = header.nextOffset
	return
}

func (fe *fileExec) write(readWriter io.ReadWriteSeeker, ft *fileAddTask) {
	var (
		err          error
		writeOffsets = make([]int64, 0)
		addResult    = fileAddTaskResult{writeOffset: -1, lastOffset: ft.lastOffset}
	)
	defer func() {
		rerr := recover()
		if rerr != nil {
			if fe.logger != nil {
				fe.logger.Errorf("[%d-%d]write task failed: %v, stack: %s", fe.level, ft.pos, rerr, system.Stack())
			}
			err = errors.Errorf("[%d-%d]write task failed: %v", fe.level, ft.pos, rerr)
		}
		addResult.err = err
		// note: not need sync
		ft.finish <- addResult
		if err != nil {
			// 释放空间
			nf := fileWriterNextOffset{file: readWriter, finish: make(chan struct{}, 1)}
			for _, writeOffset := range writeOffsets {
				nf.offset = writeOffset
				fe.freeChannel <- nf
				<-nf.finish
			}
		}
		fe.writerReady <- readWriter
	}()
	var (
		tasks         = ft.task
		seekOffset    int64
		offsetChannel = fileReaderNextOffset{file: readWriter, finish: make(chan int64, 1)}
		prevHeader    *taskHeader
		currHeader    *taskHeader
	)
nextWriter:
	if len(tasks) == 0 {
		return
	}
	currLength := 0
	offset := addResult.lastOffset
	if prevHeader != nil || offset < 0 {
		// 获取空间
		fe.nextOffsetChannel <- offsetChannel
		offset = <-offsetChannel.finish
		if offset < 0 {
			err = errors.Errorf("[%d-%d]write seek task-header failed", fe.level, ft.pos)
			return
		}
		writeOffsets = append(writeOffsets, offset)
		// 跳过头数据【后面数量确定后写入】
		seekOffset = offset + taskHeaderSize
		currHeader = &taskHeader{pos: uint16(ft.pos), lastOffset: taskHeaderSize, nextOffset: -1}
	} else {
		currHeader, err = fe._readTaskHeader(readWriter, offset)
		if err != nil {
			err = errors.WithMessagef(err, "[%d-%d]write read task-header failed", fe.level, ft.pos)
			return
		}
		currLength = int(currHeader.lastOffset)
		seekOffset = offset + int64(currHeader.lastOffset)
	}
	_, err = readWriter.Seek(seekOffset, io.SeekStart)
	if err != nil {
		err = errors.WithMessagef(err, "[%d-%d]write seek task failed", fe.level, ft.pos)
		return
	}
	// 写入具体数据
	for i := 0; i <= len(tasks); i++ {
		if i < len(tasks) {
			bs, merr := fe.proto.Marshal(tasks[i])
			if merr != nil {
				err = errors.WithMessagef(merr, "[%s]task marshal failed", tasks[i].Uid())
				return
			}
			if len(bs) == 0 {
				continue
			}
			// 使用1个字节存储长度
			bs = append(make([]byte, 1), bs...)
			bs[0] = byte(len(bs) - 1)
			currLength += len(bs)
			if currLength <= taskBodySize || currHeader.num == 0 {
				_, err = readWriter.Write(bs)
				if err != nil {
					err = errors.WithMessagef(err, "[%d-%d]write task failed", fe.level, ft.pos)
					return
				}
				currHeader.num++
				currHeader.lastOffset += uint16(len(bs))
				continue
			}
		}
		if i > 0 {
			// 是否是首次
			if addResult.writeOffset == -1 {
				addResult.writeOffset = offset
			}
			// 写入头部数据
			err = fe._writeTaskHeader(readWriter, offset, currHeader)
			if err != nil {
				err = errors.WithMessagef(err, "[%d-%d]write task-header failed", fe.level, ft.pos)
				return
			}
			if prevHeader != nil {
				// 如果存在前置，更新前置的nextOffset为当前
				prevHeader.nextOffset = offset
				err = fe._writeTaskHeader(readWriter, addResult.lastOffset, prevHeader)
				if err != nil {
					err = errors.WithMessagef(err, "[%d-%d]write task-header-reset failed", fe.level, ft.pos)
					return
				}
			}
			addResult.lastOffset = offset
			tasks = tasks[i:]
		}
		prevHeader = currHeader
		goto nextWriter
	}
}

func (fe *fileExec) read(reader io.ReadWriteSeeker, ft *fileReceiveTask) {
	var (
		err        error
		tasks      []task.Task
		currOffset = ft.offset
		nextOffset = currOffset
	)
	defer func() {
		rerr := recover()
		if rerr != nil {
			if fe.logger != nil {
				fe.logger.Errorf("[%d-%d]write task failed: %v, stack: %s", fe.level, ft.pos, rerr, system.Stack())
			}
			err = errors.Errorf("[%d-%d]write task failed: %v", fe.level, ft.pos, rerr)
		}
		ft.finish <- fileReceiveResult{task: tasks, nextOffset: nextOffset, err: err}
		if nextOffset != currOffset {
			nf := fileWriterNextOffset{file: reader, offset: currOffset, finish: make(chan struct{}, 1)}
			fe.freeChannel <- nf
			<-nf.finish
		}
		fe.readerReady <- reader
	}()
	header, err := fe._readTaskHeader(reader, currOffset)
	if err != nil {
		err = errors.WithMessagef(err, "[%d-%d]read seek task-header failed", fe.level, ft.pos)
		return
	}
	if header.num == 0 {
		nextOffset = header.nextOffset
		return
	}
	bs := make([]byte, header.lastOffset-taskHeaderSize)
	_, err = reader.Read(bs)
	if err != nil {
		err = errors.WithMessagef(err, "[%d-%d]read task failed", fe.level, ft.pos)
		return
	}
	tasks = make([]task.Task, 0, header.num)
	for len(bs) > 0 {
		l := int(bs[0])
		if len(bs) < l+1 {
			if fe.logger != nil {
				fe.logger.Errorf("[%d-%d]read last task invalid: %d", fe.level, ft.pos, currOffset)
			}
			break
		}
		tk, uerr := fe.proto.UnMarshal(bs[1 : l+1])
		bs = bs[l+1:]
		if uerr != nil {
			if fe.logger != nil {
				fe.logger.Errorf("[%d-%d]read task invalid: %s", fe.level, ft.pos, uerr.Error())
			}
			continue
		}
		tasks = append(tasks, tk)
	}
	nextOffset = header.nextOffset
}

func (fe *fileExec) run() {
	tc := time.NewTicker(defaultFileCheckTime)
	defer func() {
		rerr := recover()
		if rerr != nil && fe.logger != nil {
			fe.logger.Errorf("[%d]file store panic: %v, stack: %s", fe.level, rerr, system.Stack())
		}
		tc.Stop()
		// todo: 通知其他服务
	}()
	err := fe._mkDir()
	if err != nil {
		panic(err)
	}
	var (
		// task
		ac = fe.addChannel
		rc = fe.receiveChannel
		ar *fileAddTask
		rr *fileReceiveTask
		// seek
		noc = fe.nextOffsetChannel
		foc = fe.freeChannel
	)
	for {
		select {
		case <-tc.C:
			fe._closeTimeoutFile()
		case war := <-ac:
			writer, err := fe._fetchWriter()
			if err == nil {
				go fe.write(writer, war)
				break
			}
			if err == notEnoughWriter {
				ac = nil
				ar = war
				break
			}
			war.finish <- fileAddTaskResult{err: err}
		case wrr := <-rc:
			reader, err := fe._fetchReader()
			if err == nil {
				go fe.read(reader, wrr)
				break
			}
			if err == notEnoughReader {
				rc = nil
				rr = wrr
				break
			}
			wrr.finish <- fileReceiveResult{err: err}
		case r := <-fe.readerReady:
			if rr != nil {
				wrr := rr
				rr = nil
				rc = fe.receiveChannel
				go fe.read(r, wrr)
				break
			}
			fe.reader = append(fe.reader, fileReadeWriteSeeker{
				now:  time.Now(),
				file: r,
			})
		case w := <-fe.writerReady:
			if ar != nil {
				war := ar
				ar = nil // note: 协程调度
				ac = fe.addChannel
				go fe.write(w, war) // note: 暂不接入协程池【数量是固定的】
				break
			}
			fe.readWriter = append(fe.readWriter, fileReadeWriteSeeker{
				now:  time.Now(),
				file: w,
			})
		case oc := <-noc:
			go fe.fetchNextOffset(oc.file, oc.finish)
			noc = nil
			foc = nil // fix: noc foc 存在竞争关系
		case <-fe.nextOffsetReady:
			noc = fe.nextOffsetChannel
			foc = fe.freeChannel
		case fc := <-foc:
			go fe.updateFreeOffset(fc.file, fc.offset, fc.finish)
			foc = nil
			noc = nil
		case <-fe.freeReady:
			noc = fe.nextOffsetChannel
			foc = fe.freeChannel
		case <-fe.stopChannel:
			if fe.logger != nil {
				fe.logger.Errorf("[%d]file store stopped", fe.level)
			}
			return
		}
	}
}

func (fe *fileExec) _closeTimeoutFile() {
	// note: 暂时不进行异步关闭
	for cn := 0; len(fe.reader) > 0 && cn < defaultFileCheckNum && defaultFileIdleTime < time.Since(fe.reader[0].now); cn++ {
		_ = fe.reader[0].file.(io.Closer).Close()
		fe.reader = fe.reader[1:]
		fe.currReader--
	}
	for cn := 0; len(fe.readWriter) > 0 && cn < defaultFileCheckNum && defaultFileIdleTime < time.Since(fe.readWriter[0].now); cn++ {
		_ = fe.readWriter[0].file.(io.Closer).Close()
		fe.readWriter = fe.readWriter[1:]
		fe.currWriter--
	}
}

func (fe *fileExec) _fetchWriter() (io.ReadWriteSeeker, error) {
	if len(fe.readWriter) > 0 {
		writer := fe.readWriter[0].file
		fe.readWriter = fe.readWriter[1:]
		return writer, nil
	}
	if fe.currWriter == fe.maxWriter {
		return nil, notEnoughWriter
	}
	writer, err := os.OpenFile(fmt.Sprintf("%s/data_%d.t", strings.TrimRight(fe.dir, "/"), fe.level), os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, errors.WithMessage(err, "open file failed")
	}
	fe.currWriter++
	return writer, nil
}

func (fe *fileExec) _fetchReader() (io.ReadWriteSeeker, error) {
	if len(fe.reader) > 0 {
		reader := fe.reader[0].file
		fe.reader = fe.reader[1:]
		return reader, nil
	}
	if fe.currReader == fe.maxReader {
		return nil, notEnoughReader
	}
	reader, err := os.OpenFile(fmt.Sprintf("%s/data_%d.t", strings.TrimRight(fe.dir, "/"), fe.level), os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, errors.WithMessage(err, "open file failed")
	}
	fe.currReader++
	return reader, nil
}

func (fe *fileExec) _mkDir() error {
	dir := strings.TrimRight(fe.dir, "/")
	_, err := os.Stat(dir)
	if err == nil {
		return nil
	}
	if !os.IsNotExist(err) {
		return nil
	}
	return os.Mkdir(dir, 0766)
}

type fileExecOption func(fe *fileExec)

func fileExecOptionWithDir(dir string, level int) fileExecOption {
	return func(fe *fileExec) {
		fe.dir = dir
		fe.level = level
	}
}

func fileExecOptionWithMaxFile(maxReader, maxWriter int) fileExecOption {
	return func(fe *fileExec) {
		fe.maxReader = maxReader
		fe.maxWriter = maxWriter
	}
}

func fileExecOptionWithLogger(logger system.Logger) fileExecOption {
	return func(fe *fileExec) {
		fe.logger = logger
	}
}

func fileExecOptionWithProto(proto taskProto) fileExecOption {
	return func(fe *fileExec) {
		fe.proto = proto
	}
}

func newFileExec(opts ...fileExecOption) *fileExec {
	fe := fileExec{
		addChannel:        make(chan *fileAddTask),
		receiveChannel:    make(chan *fileReceiveTask),
		stopChannel:       make(chan struct{}),
		readerReady:       make(chan io.ReadWriteSeeker, maxChannelCap),
		writerReady:       make(chan io.ReadWriteSeeker, maxChannelCap),
		freeChannel:       make(chan fileWriterNextOffset),
		freeReady:         make(chan struct{}, 1),
		nextOffsetChannel: make(chan fileReaderNextOffset),
		nextOffsetReady:   make(chan struct{}, 1),
		freeOffset:        fileOffset{writeOffset: -1, lastOffset: -1},
	}
	for _, opt := range opts {
		opt(&fe)
	}
	return &fe
}
