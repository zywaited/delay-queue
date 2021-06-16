package server

import (
	"context"
	"errors"
	"net"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	pkgerr "github.com/pkg/errors"
	"github.com/zywaited/delay-queue/inter"
	mgrpc "github.com/zywaited/delay-queue/middleware/grpc"
	mhttp "github.com/zywaited/delay-queue/middleware/http"
	"github.com/zywaited/delay-queue/protocol/pb"
	"github.com/zywaited/delay-queue/role"
	"github.com/zywaited/delay-queue/role/runner"
	"github.com/zywaited/delay-queue/role/task"
	"github.com/zywaited/delay-queue/server/api/grpc"
	"github.com/zywaited/delay-queue/server/api/http"
	"github.com/zywaited/delay-queue/server/service"
	ogrpc "google.golang.org/grpc"
)

type protocolOption struct {
}

func NewProtocolOption() *protocolOption {
	return &protocolOption{}
}

func (po *protocolOption) runProtocol(dq *DelayQueue) error {
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
		service.HandleOptionWithWait(dq.c.C.Services.Wait),
		service.HandleOptionWithGP(dq.gp),
		service.HandleOptionWithIdCreator(dq.idCreator),
	)
	c := make(chan error)
	defer close(c)
	closed := 0
	for _, schema := range dq.c.C.Services.Types {
		schema = strings.ToUpper(strings.TrimSpace(schema))
		switch schema {
		case inter.GRPCSchema:
			po.runGRPCServer(dq, c, h)
		case inter.HTTPSchema:
			po.runHTTPServer(dq, c, h)
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

func (po *protocolOption) runGRPCServer(dq *DelayQueue, c chan error, handler pb.DelayQueueServer) {
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

func (po *protocolOption) runHTTPServer(dq *DelayQueue, c chan error, handler pb.DelayQueueServer) {
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

func (po *protocolOption) Run(dq *DelayQueue) error {
	go func() {
		err := po.runProtocol(dq)
		if err != nil && dq.c.CB.Logger != nil {
			dq.c.CB.Logger.Infof("SERVER PROTOCOL STOP: %v", err)
		}
	}()
	return nil
}

func (po *protocolOption) Stop(_ *DelayQueue) error {
	return nil
}
