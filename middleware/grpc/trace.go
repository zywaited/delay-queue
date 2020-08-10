package grpc

import (
	"context"

	uuid "github.com/satori/go.uuid"
	"github.com/zywaited/delay-queue/middleware"
	"google.golang.org/grpc"
)

func XTrace() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp interface{}, err error) {
		return handler(context.WithValue(ctx, middleware.TraceIdKey, uuid.NewV4().String()), req)
	}
}
