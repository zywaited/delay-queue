package role

import "errors"

var (
	ErrContextCancel = errors.New("context cancel")
	ErrServerExit    = errors.New("server exit")
)
