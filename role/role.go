package role

type Type int

const (
	Timer Type = 1 << iota

	Worker
)

type StopType int

const (
	ForceSTW = iota

	GraceFulST
)

const (
	StatusRunning int32 = iota

	StatusInitialized

	StatusGraceFulST

	StatusForceSTW
)

type Launcher interface {
	Run() error

	Stop(StopType) error
}

type LauncherStatus interface {
	Running() bool
	Stopped() bool
}
