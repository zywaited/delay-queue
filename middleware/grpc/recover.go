package grpc

import (
	"context"

	"github.com/save95/xerror"
	"github.com/save95/xerror/xcode"
	"github.com/zywaited/delay-queue/parser/system"
	"google.golang.org/grpc"
)

func Recover(logger system.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		defer func() {
			if rerr := recover(); rerr != nil {
				if logger != nil {
					logger.Errorf("GRPC[Recovery] panic recovered: %v, stack: %s", err, system.Stack())
				}
				err = xerror.WithXCodeMessage(xcode.InternalServerError, "server panic")
			}
		}()
		resp, err = handler(ctx, req)
		return
	}
}
