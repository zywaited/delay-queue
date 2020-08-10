package grpc

import (
	"context"
	"encoding/json"
	"time"

	"github.com/save95/xerror"
	"github.com/zywaited/delay-queue/middleware"
	"github.com/zywaited/delay-queue/parser/system"
	"google.golang.org/grpc"
)

func Log(logger system.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp interface{}, err error) {
		traceId := ctx.Value(middleware.TraceIdKey)
		bs, _ := json.Marshal(req)
		logger.Infof("[%s][%s] Req: %s", traceId, info.FullMethod, string(bs))
		st := time.Now()
		resp, err = handler(ctx, req)
		cost := time.Since(st)
		if err == nil {
			logger.Infof("[%s Cost times: %s", traceId, cost)
			return
		}
		code := 0
		if xerr, ok := err.(xerror.XError); ok {
			code = xerr.ErrorCode()
		}
		logger.Infof("[%s] Cost times: %s, code: %d, err: %s", traceId, cost, code, err.(error).Error())
		return
	}
}
