package system

import (
	"runtime"
)

func Stack() string {
	stackBuf := make([]byte, 4096)
	n := runtime.Stack(stackBuf, false)
	return string(stackBuf[:n])
}
