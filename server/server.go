package server

import (
	"context"
	"errors"
	"flag"
	"net"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	pkgerr "github.com/pkg/errors"
	"github.com/zywaited/delay-queue/inter"
	mgrpc "github.com/zywaited/delay-queue/middleware/grpc"
	mhttp "github.com/zywaited/delay-queue/middleware/http"
	"github.com/zywaited/delay-queue/protocol/pb"
	"github.com/zywaited/delay-queue/repository/redis"
	"github.com/zywaited/delay-queue/role"
	"github.com/zywaited/delay-queue/role/reload"
	"github.com/zywaited/delay-queue/role/runner"
	"github.com/zywaited/delay-queue/role/task"
	"github.com/zywaited/delay-queue/role/task/store"
	"github.com/zywaited/delay-queue/role/timer"
	"github.com/zywaited/delay-queue/role/timer/sorted"
	tw "github.com/zywaited/delay-queue/role/timer/timing-wheel"
	"github.com/zywaited/delay-queue/role/worker"
	"github.com/zywaited/delay-queue/server/api/grpc"
	"github.com/zywaited/delay-queue/server/api/http"
	"github.com/zywaited/delay-queue/server/service"
	"github.com/zywaited/delay-queue/transport"
	trhttp "github.com/zywaited/delay-queue/transport/http"
	"github.com/zywaited/go-common/xcopy"
	ogrpc "google.golang.org/grpc"
)

type DelayQueue struct {
	c     *inter.ConfigData
	store role.DataStore
	base  time.Duration
	cp    *xcopy.XCopy

	timer      timer.Scanner
	convert    role.PbConvertTask
	rq         runner.ReadyQueue
	worker     role.Launcher
	workerId   string
	timerId    string
	transports transport.TransporterM
}

func NewDelayQueue() *DelayQueue {
	return &DelayQueue{cp: xcopy.NewCopy()}
}

func (dq *DelayQueue) Run() error {
	err := dq.config()
	if err != nil {
		return pkgerr.WithMessage(err, "读取配置失败")
	}
	if err = dq.initId(); err != nil {
		return err
	}
	_ = dq.initTransporters()
	dq.convert = role.NewDefaultPbConvertTask(task.AcTaskPoolFactory(task.DefaultTaskPoolFactory))
	if err = dq.initStore(); err != nil {
		return err
	}
	dq.base = time.Millisecond
	switch dq.c.C.BaseLevel {
	case "second":
		dq.base = time.Second
	}
	// 服务启动放到最后
	err = dq.server()
	if err != nil {
		return pkgerr.WithMessage(err, "服务初始化失败")
	}
	// loop
	// todo 后续加入信号监听
	select {}
}

func (dq *DelayQueue) config() error {
	configFile := ""
	flag.StringVar(&configFile, "f", "config/config.toml", "config file")
	flag.Parse()
	gc := inter.NewGenerateConfigWithDefaultAfters(inter.GenerateConfigWithConfigName(configFile))
	if err := gc.Run(); err != nil {
		return err
	}
	dq.c = gc.ConfigData()
	return nil
}

func (dq *DelayQueue) server() (err error) {
	err = dq.timerServer()
	if err != nil {
		return
	}
	dq.initRunner()
	if err = dq.reloadServer(); err != nil {
		return
	}
	go func() {
		err = dq.runProtocol()
		if err != nil && dq.c.CB.Logger != nil {
			dq.c.CB.Logger.Infof("SERVER PROTOCOL STOP: %v", err)
		}
	}()
	_ = dq.runWorker()
	return
}

func (dq *DelayQueue) timerServer() error {
	if dq.c.C.Role&uint(role.Timer) == 0 {
		return nil
	}
	sr := (timer.Scanner)(nil)
	switch dq.c.C.Timer.St {
	case string(tw.ServerName):
		sr = tw.NewServer(
			tw.NewServerConfig(tw.ServerConfigWithFactory(task.AcPoolFactory(task.DefaultTaskPoolFactory))),
			tw.OptionWithSlotNum(dq.c.C.Timer.TimingWheel.SlotNum),
			tw.OptionWithLogger(dq.c.CB.Logger),
			tw.OptionWithMaxLevel(dq.c.C.Timer.TimingWheel.MaxLevel),
			tw.OptionWithConfigScale(time.Duration(dq.c.C.ConfigScale)*dq.base),
			tw.OptionWithScaleLevel(time.Duration(dq.c.C.Timer.ConfigScaleLevel)*dq.base),
			tw.OptionWithNewTaskStore(store.Handler(store.DefaultStoreName)),
		)
	case string(sorted.ServerName):
		sr = sorted.NewServer(
			sorted.ServerConfigWithLogger(dq.c.CB.Logger),
			sorted.ServerConfigWithScale(time.Duration(dq.c.C.ConfigScale/dq.c.C.Timer.ConfigScaleLevel)*dq.base),
			sorted.ServerConfigWithStore(store.Handler(store.SortedListStoreName)),
		)
	}
	dq.timer = sr
	return pkgerr.WithMessage(sr.Run(), "扫描器启动失败")
}

func (dq *DelayQueue) gRpcServer(c chan error, handler pb.DelayQueueServer) {
	interceptors := []ogrpc.UnaryServerInterceptor{
		mgrpc.Recover(dq.c.CB.Logger),
		mgrpc.XTrace(),
		mgrpc.Log(dq.c.CB.Logger),
	}
	last := len(interceptors) - 1
	chainUnary := func(
		ctx context.Context,
		req interface{},
		info *ogrpc.UnaryServerInfo,
		handler ogrpc.UnaryHandler,
	) (interface{}, error) {
		var chainHandler ogrpc.UnaryHandler
		cur := 0
		chainHandler = func(currentCtx context.Context, currentReq interface{}) (interface{}, error) {
			if cur == last {
				return handler(currentCtx, currentReq)
			}
			cur++
			resp, err := interceptors[cur](currentCtx, currentReq, info, chainHandler)
			cur--
			return resp, err
		}
		return interceptors[0](ctx, req, info, chainHandler)
	}
	g := ogrpc.NewServer(ogrpc.UnaryInterceptor(chainUnary))
	pb.RegisterDelayQueueServer(g, grpc.NewServer(handler))
	go func() {
		if dq.c.CB.Logger != nil {
			dq.c.CB.Logger.Infof("GRPC SERVER START: %s", dq.c.C.Services.GRPC.Addr)
		}
		l, err := net.Listen("tcp", dq.c.C.Services.GRPC.Addr)
		if err != nil {
			c <- pkgerr.WithMessage(err, "GRPC服务监听异常")
		}
		c <- pkgerr.WithMessage(g.Serve(l), "GRPC服务异常退出")
	}()
}

func (dq *DelayQueue) httpServer(c chan error, handler pb.DelayQueueServer) {
	g := gin.Default()
	g.Use(mhttp.Recovery(dq.c.CB.Logger))
	g.Use(mhttp.XTrace())
	g.Use(mhttp.Log(dq.c.CB.Logger))
	g.Use(mhttp.Cors())

	s := http.NewServer(handler)
	rg := g.Group("task")
	rg.POST("/add", s.Add)
	rg.GET("/get", s.Get)
	// 可能部分服务器没有当前方法
	rg.DELETE("/remove", s.Remove)
	rg.POST("/remove", s.Remove)
	go func() {
		if dq.c.CB.Logger != nil {
			dq.c.CB.Logger.Infof("HTTP SERVER START: %s", dq.c.C.Services.HTTP.Addr)
		}
		err := g.Run(dq.c.C.Services.HTTP.Addr)
		c <- pkgerr.WithMessage(err, "HTTP服务异常退出")
	}()
}

// 初始化存储
func (dq *DelayQueue) initStore() error {
	rcs := []redis.ConfigOption{
		redis.ConfigWithPrefix(dq.c.C.DataSource.Redis.Prefix),
		redis.ConfigWithName(dq.c.C.DataSource.Redis.Name),
		redis.ConfigWithId(dq.timerId),
		redis.ConfigWithConvert(dq.convert),
		redis.ConfigWithCopy(dq.cp),
	}
	switch dq.c.C.DataSource.Dst {
	case inter.RedisStore:
		dq.store = redis.NewTWStore(dq.c.CB.Redis, rcs...)
	default:
		return errors.New("未知的存储类型")
	}

	// sorted list server
	if dq.c.C.Timer.St == string(sorted.ServerName) {
		// 需要先注入存储
		store.RegisterHandler(
			store.SortedListStoreName,
			store.NewTaskPoolStore(
				store.NewSortedList(redis.NewSortedSet(dq.c.CB.Redis, dq.store, rcs...)).NewSortedList,
				store.NewSortedListStoreIterator,
			).NewTaskStore,
		)
	}

	// init ready-queue
	rcs[2] = redis.ConfigWithId(dq.workerId) // 重置ID
	dq.rq = redis.NewReadyQueue(dq.c.CB.Redis, dq.store, rcs...)
	return nil
}

func (dq *DelayQueue) initRunner() {
	rr := runner.NewRunner(
		dq.rq,
		dq.store,
		runner.OptionWithLogger(dq.c.CB.Logger),
		runner.OptionWithTimer(dq.timer),
		runner.OptionWithMaxCheckTime(dq.c.C.Timer.MaxCheckTime),
		runner.OptionWithCheckMulti(dq.c.C.Timer.CheckMulti),
		runner.OptionWithTaskPool(task.AcTaskPoolFactory(task.DefaultTaskPoolFactory)),
	)
	runner.RegisterRunner(runner.DefaultRunnerName, rr.Run)
}

func (dq *DelayQueue) runProtocol() error {
	if dq.c.C.Role&uint(role.Timer) == 0 {
		return nil
	}
	h := service.NewHandle(
		service.HandleOptionWithXCopy(dq.cp),
		service.HandleOptionWithLogger(dq.c.CB.Logger),
		service.HandleOptionWithBaseTime(dq.base),
		service.HandleOptionWithRunner(runner.AcRunner(runner.DefaultRunnerName)),
		service.HandleOptionWithTimeOut(time.Duration(dq.c.C.Timer.Timeout)), // 这里不能乘以base，内部自己做了
		service.HandleOptionWithTp(task.AcTaskPoolFactory(task.DefaultTaskPoolFactory)),
		service.HandleOptionWithTimer(dq.timer),
		service.HandleOptionWithStore(dq.store),
		service.HandleOptionTransporters(dq.transports),
	)
	c := make(chan error)
	defer close(c)
	closed := 0
	for _, schema := range dq.c.C.Services.Types {
		schema = strings.ToUpper(strings.TrimSpace(schema))
		switch schema {
		case inter.GRPCSchema:
			dq.gRpcServer(c, h)
		case inter.HTTPSchema:
			dq.httpServer(c, h)
		}
	}
	for err := range c {
		if dq.c.CB.Logger != nil {
			dq.c.CB.Logger.Infof("SERVER STOP: %v", err)
		}
		closed++
		if closed >= len(dq.c.C.Services.Types) {
			return errors.New("对外接口服务启动异常")
		}
	}
	return nil
}

func (dq *DelayQueue) runWorker() error {
	if dq.c.C.Role&uint(role.Worker) == 0 {
		return nil
	}
	w := worker.NewScanner(
		worker.OptionWithLogger(dq.c.CB.Logger),
		worker.OptionsWithConfigScale(time.Duration(dq.c.C.ConfigScale)*dq.base),
		worker.OptionWithStore(dq.store),
		worker.OptionsWithReadyQueue(dq.rq),
		worker.OptionsWithRepeatedTimes(int(dq.c.C.Worker.RepeatedTimes)),
		worker.OptionsWithTimeout(time.Duration(dq.c.C.Worker.Timeout)*dq.base),
		worker.OptionsWithMultiNum(int(dq.c.C.Worker.MultiNum)),
		worker.OptionsWithRetryTimes(dq.c.C.Worker.RetryTimes),
		worker.OptionWithTransporters(dq.transports),
	)
	dq.worker = w
	return pkgerr.WithMessage(w.Run(), "Worker启动失败")
}

func (dq *DelayQueue) initId() error {
	c := (<-chan time.Time)(nil)
	if dq.c.C.GenerateId.Timeout > 0 {
		c = time.NewTimer(time.Duration(dq.c.C.GenerateId.Timeout) * dq.base).C
	}
	d := make(chan struct{})
	go func() {
		dq.timerId, dq.workerId = role.AcGenerateId()()
		d <- struct{}{}
	}()
	select {
	case <-d:
	case <-c:
		return errors.New("generate id timeout")
	}
	if dq.c.CB.Logger != nil {
		dq.c.CB.Logger.Infof("SERVER NAME: %s: %s", dq.timerId, dq.workerId)
	}
	return nil
}

func (dq *DelayQueue) initTransporters() error {
	timeout := time.Duration(0)
	if dq.c.C.Role&uint(role.Timer) != 0 {
		timeout = time.Duration(dq.c.C.Timer.Timeout)
	}
	if dq.c.C.Role&uint(role.Worker) == 0 {
		timeout = time.Duration(dq.c.C.Worker.Timeout)
	}
	timeout *= dq.base
	dq.transports = make(transport.TransporterM)
	th := trhttp.NewSender(trhttp.SenderOptionWithTimeout(timeout))
	dq.transports[transport.TransporterType(trhttp.SendersType)] = th
	dq.transports[transport.TransporterType(trhttp.SenderHttpsType)] = th
	return nil
}

func (dq *DelayQueue) reloadServer() error {
	if dq.c.C.Role&uint(role.Timer) == 0 {
		return nil
	}
	if dq.timer == nil || runner.AcRunner(runner.DefaultRunnerName) == nil {
		return errors.New("timer or runner not init")
	}
	if _, ok := dq.store.(*redis.TWStore); !ok {
		return nil
	}
	rs := reload.NewServer(
		reload.ServerConfigWithLogger(dq.c.CB.Logger),
		reload.ServerConfigWithReload(redis.NewReload(dq.store.(*redis.TWStore), dq.convert)),
		reload.ServerConfigWithReloadGN(dq.c.C.Timer.TimingWheel.ReloadGoNum),
		reload.ServerConfigWithReloadScale(time.Duration(dq.c.C.Timer.TimingWheel.ReloadConfigScale)*dq.base),
		reload.ServerConfigWithReloadPerNum(dq.c.C.Timer.TimingWheel.ReloadPerNum),
		reload.ServerConfigWithTimer(dq.timer),
		reload.ServerConfigWithRunner(runner.AcRunner(runner.DefaultRunnerName)),
	)
	return pkgerr.WithMessage(rs.Run(), "Reload启动失败")
}
