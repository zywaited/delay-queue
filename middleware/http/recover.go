package http

import (
	"github.com/gin-gonic/gin"
	"github.com/save95/xerror/xcode"
	"github.com/zywaited/delay-queue/parser/system"
)

func Recovery(logger system.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				if logger != nil {
					logger.Errorf("HTTP[Recovery] panic recovered: %v, stack: %s", err, system.Stack())
				}
				// 500
				c.Status(xcode.InternalServerError.HttpStatus())
			}
		}()
		c.Next()
	}
}
