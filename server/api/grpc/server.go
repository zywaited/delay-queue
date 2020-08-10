package grpc

import (
	"context"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/zywaited/delay-queue/protocol/pb"
)

// 只是为了区分(转发)
type Server struct {
	s pb.DelayQueueServer
}

func NewServer(s pb.DelayQueueServer) *Server {
	return &Server{s: s}
}

func (s *Server) Add(ctx context.Context, req *pb.AddReq) (*pb.AddResp, error) {
	return s.s.Add(ctx, req)
}

func (s *Server) Get(ctx context.Context, req *pb.RetrieveReq) (*pb.Task, error) {
	return s.s.Get(ctx, req)
}

func (s *Server) Remove(ctx context.Context, req *pb.RemoveReq) (*empty.Empty, error) {
	return s.s.Remove(ctx, req)
}
