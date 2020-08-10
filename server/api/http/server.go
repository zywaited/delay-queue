package http

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/save95/xerror"
	"github.com/save95/xerror/xcode"
	"github.com/zywaited/delay-queue/middleware"
	"github.com/zywaited/delay-queue/protocol/pb"
)

type Server struct {
	s pb.DelayQueueServer
}

func NewServer(s pb.DelayQueueServer) *Server {
	return &Server{s: s}
}

func (s *Server) error(c *gin.Context, err error) {
	defer c.Set(middleware.HTTPMiddlewareLogErrorKey, err) // 中间件日志打印
	xerr, ok := err.(xerror.XError)
	if !ok {
		c.Status(xcode.InternalServerError.HttpStatus())
		return
	}
	c.Status(xerr.HttpStatus())
}

func (s *Server) Add(c *gin.Context) {
	addReq := &pb.AddReq{}
	if err := c.ShouldBindJSON(addReq); err != nil {
		c.Status(xcode.RequestParamError.HttpStatus())
		return
	}
	traceId, _ := c.Get(middleware.TraceIdKey)
	resp, err := s.s.Add(context.WithValue(context.Background(), middleware.TraceIdKey, traceId), addReq)
	if err != nil {
		s.error(c, err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (s *Server) Get(c *gin.Context) {
	req := &pb.RetrieveReq{}
	if err := c.ShouldBindJSON(req); err != nil {
		c.Status(xcode.RequestParamError.HttpStatus())
		return
	}
	traceId, _ := c.Get(middleware.TraceIdKey)
	resp, err := s.s.Get(context.WithValue(context.Background(), middleware.TraceIdKey, traceId), req)
	if err != nil {
		s.error(c, err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (s *Server) Remove(c *gin.Context) {
	req := &pb.RemoveReq{}
	if err := c.ShouldBindJSON(req); err != nil {
		c.Status(xcode.RequestParamError.HttpStatus())
		return
	}
	traceId, _ := c.Get(middleware.TraceIdKey)
	_, err := s.s.Remove(context.WithValue(context.Background(), middleware.TraceIdKey, traceId), req)
	if err != nil {
		s.error(c, err)
		return
	}
	c.Status(http.StatusOK)
}
