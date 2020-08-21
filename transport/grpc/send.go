package grpc

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/ptypes/any"
	pkgerr "github.com/pkg/errors"
	mgrpc "github.com/zywaited/delay-queue/middleware/grpc"
	"github.com/zywaited/delay-queue/parser/system"
	"github.com/zywaited/delay-queue/protocol/pb"
	"google.golang.org/grpc"
)

const SendersType = "GRPC"

type SenderOption func(*Sender)

func SenderOptionWithTimeout(timeout time.Duration) SenderOption {
	return func(s *Sender) {
		s.timeout = timeout
	}
}

func SenderOptionWithIdleTime(idleTime time.Duration) SenderOption {
	return func(s *Sender) {
		s.idleTime = idleTime
	}
}

func SenderOptionWithCheckTime(checkTime time.Duration) SenderOption {
	return func(s *Sender) {
		s.checkTime = checkTime
	}
}

func SenderOptionWithLogger(logger system.Logger) SenderOption {
	return func(s *Sender) {
		s.logger = logger
	}
}

func SenderOptionWithConnectTime(connectTime time.Duration) SenderOption {
	return func(s *Sender) {
		s.connectTime = connectTime
	}
}

const (
	defaultConnectTimeout = time.Second
	defaultCheckTime      = time.Minute
	defaultIdleTime       = time.Minute * 5
	defaultCheckNum       = 50 // 每次处理50个链接
)

type ct struct {
	last time.Duration
	gc   *grpc.ClientConn
	// 该字段的作用：
	// 1: 多协程只会创建同一个链接 （阻塞）
	// 2: 为了能够锁住sm的时间短
	c   chan struct{}
	num int32
	key string
}

type Sender struct {
	locker sync.RWMutex
	sm     map[string]*ct

	timeout     time.Duration
	idleTime    time.Duration
	checkTime   time.Duration
	connectTime time.Duration
	logger      system.Logger

	ctp *sync.Pool
}

func NewSender(opts ...SenderOption) *Sender {
	s := &Sender{
		sm:          make(map[string]*ct),
		connectTime: defaultConnectTimeout,
		checkTime:   defaultCheckTime,
		idleTime:    defaultIdleTime,
		ctp: &sync.Pool{New: func() interface{} {
			return &ct{}
		}},
	}
	for _, opt := range opts {
		opt(s)
	}
	go s.check()
	return s
}

func (s *Sender) check() {
	if s.checkTime <= 0 || s.idleTime <= 0 {
		return
	}
	t := time.NewTicker(s.checkTime)
	for {
		select {
		case <-t.C:
			s.closeTimeoutC()
		}
	}
}

func (s *Sender) closeTimeoutC() {
	defer func() {
		_ = recover()
	}()
	now := time.Duration(time.Now().UnixNano())
	// 为了速度，遍历只获取过期链接
	// 这里拆分成两步
	// 1: 读锁获取过期链接
	// 2: 写锁获取真正过期的链接
	// 这么做的目的只是在链接能够命中的时候效率更高，但新建链接无变化
	s.locker.RLock()
	num := 0
	cts := make([]*ct, 0)
	for _, c := range s.sm {
		if c == nil {
			cts = append(cts, c)
			continue
		}
		if atomic.LoadInt32(&c.num) > 0 {
			continue
		}
		if c.gc != nil {
			if now-c.last < s.idleTime {
				continue
			}
			cts = append(cts, c)
		}
		num++
		if num >= defaultCheckNum {
			break
		}
	}
	s.locker.RUnlock()
	if len(cts) == 0 {
		return
	}
	// 这里才真正处理数据
	rs := make([]*ct, 0, len(cts))
	s.locker.Lock()
	for _, c := range cts {
		if atomic.LoadInt32(&c.num) == 0 && now-c.last >= s.idleTime {
			rs = append(rs, c)
			delete(s.sm, c.key)
		}
	}
	s.locker.Unlock()
	if len(rs) == 0 {
		return
	}
	// 方便打印日志
	keys := make([]string, 0, len(rs))
	for _, c := range rs {
		keys = append(keys, c.key)
		err := c.gc.Close()
		if err != nil && s.logger != nil {
			s.logger.Infof("close timeout grpc client failed: %s-%s", c.key, err.Error())
		}
		c.gc = nil
		c.num = 0
		c.key = ""
		s.ctp.Put(c)
	}
	if s.logger != nil {
		s.logger.Infof("close timeout grpc client: %v", keys)
	}
}

func (s *Sender) Send(addr *pb.CallbackInfo, req *pb.CallBackReq) error {
	s.locker.RLock()
	c := s.sm[addr.Address]
	if c != nil {
		c.last = time.Duration(time.Now().UnixNano())
		atomic.AddInt32(&c.num, 1)
	}
	s.locker.RUnlock()
	ctx := context.Background()
	if s.timeout > 0 {
		tx, cancel := context.WithTimeout(ctx, s.timeout)
		defer cancel()
		ctx = tx
	}
	if c == nil {
		s.locker.Lock()
		// double check
		c = s.sm[addr.Address]
		if c == nil {
			c = s.ctp.Get().(*ct)
			c.last = time.Duration(time.Now().UnixNano())
			c.c = make(chan struct{})
			c.num = 1
			c.key = addr.Address
			s.sm[addr.Address] = c
			go func() {
				defer func() {
					_ = recover()
					close(c.c)
				}()
				tx := context.Background()
				if s.connectTime > 0 {
					tctx, cancel := context.WithTimeout(context.Background(), s.connectTime)
					defer cancel()
					tx = tctx
				}
				gc, err := grpc.DialContext(tx, addr.Address,
					grpc.WithChainUnaryInterceptor(mgrpc.UnaryClientRecoverInterceptor(s.logger)),
					grpc.WithInsecure(),
					grpc.WithBlock(),
				)
				if err == nil {
					c.gc = gc
				}
			}()
		}
		s.locker.Unlock()
	}
	<-c.c // 确保初始化完成
	defer atomic.AddInt32(&c.num, -1)
	if c.gc == nil {
		// 超时
		return errors.New("connect timeout or grpc dial error")
	}
	req.Url = s.uri(addr)
	var err error
	if addr.Path == "" {
		// 默认使用该服务
		_, err = pb.NewCallbackClient(c.gc).Send(ctx, req)
	} else {
		// 指定服务端，那只有直接走逻辑调用
		// 规定：返回值必须是empty.Empty
		out := new(any.Any)
		err = c.gc.Invoke(ctx, "/"+strings.TrimLeft(addr.Path, "/"), req, out)
	}
	return pkgerr.WithMessage(err, "call grpc faileds")
}

func (s *Sender) Valid(addr *pb.CallbackInfo) error {
	return nil
}

func (s *Sender) uri(addr *pb.CallbackInfo) string {
	return strings.ToLower(strings.TrimSpace(addr.Schema)) +
		"://" + strings.TrimSpace(addr.Address) + "/" +
		strings.TrimLeft(strings.TrimSpace(addr.Path), "/")
}
