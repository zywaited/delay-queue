package role

import "context"

type GenerateIds interface {
	Launcher
	Timer(context.Context) (string, error)
	Worker(context.Context) (string, error)
	Id(context.Context) (int64, error)
}
