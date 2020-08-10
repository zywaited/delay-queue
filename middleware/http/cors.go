package http

import (
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

func Cors() gin.HandlerFunc {
	return cors.New(getCorsConfig())
}

func getCorsConfig() cors.Config {
	return cors.Config{
		AllowOriginFunc: func(origin string) bool {
			return true
		},
		AllowMethods: []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowHeaders: []string{
			"Origin", "Content-Type", "Accept", "User-Agent", "Cookie", "Authorization",
			"X-Auth-Token", "X-Requested-With", "x-wechat-type", "x-platform",
		},
		AllowCredentials: true,
		ExposeHeaders: []string{
			"Authorization", "Content-MD5",
			// 分页响应头
			"Link", "X-More-Resource", "X-Pagination-Info", "X-Total-Count", "X-Has-More",
		},
		MaxAge: 12 * time.Hour,
	}
}
