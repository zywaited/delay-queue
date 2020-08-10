package http

import (
	"github.com/gin-gonic/gin"
	uuid "github.com/satori/go.uuid"
	"github.com/zywaited/delay-queue/middleware"
)

func XTrace() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Set(middleware.TraceIdKey, uuid.NewV4().String())
		c.Next()
	}
}
