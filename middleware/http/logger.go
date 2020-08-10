package http

import (
	"net/http/httputil"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/save95/xerror"
	"github.com/zywaited/delay-queue/middleware"
	"github.com/zywaited/delay-queue/parser/system"
)

func Log(logger system.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		dumpReq, _ := httputil.DumpRequest(c.Request, true)
		traceId, _ := c.Get(middleware.TraceIdKey)
		if logger != nil {
			logger.Infof("[%s]HTTP[LOG] Receive request: %s", traceId, string(dumpReq))
		}
		begin := time.Now()
		c.Next()
		if logger == nil {
			return
		}
		cost := time.Since(begin)
		err, _ := c.Get(middleware.HTTPMiddlewareLogErrorKey)
		if err == nil {
			logger.Infof("[%s]HTTP[LOG] Cost times: %s", traceId, cost)
			return
		}
		code := 0
		if xerr, ok := err.(xerror.XError); ok {
			code = xerr.ErrorCode()
		}
		logger.Infof(
			"[%s]HTTP[LOG] Cost times: %s, code: %d, err: %s",
			traceId, cost, code, err.(error).Error(),
		)
	}
}
