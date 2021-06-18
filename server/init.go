package server

type InitStoreFunc func(*DelayQueue) error

type InitOption interface {
	Run(*DelayQueue) error
	Stop(*DelayQueue) error
}

func AcquireServerInits() []InitOption {
	return []InitOption{
		NewConfigInitOption(),
		NewBaseLevelOption(),
		NewPoolOption(),
		NewCurrentIdOption(),
		NewTransportersOption(),
		NewStoreOption(),
		NewTimerOption(),
		NewRunnerOption(),
		NewReloadOption(),
		NewProtocolOption(),
		NewWorkerOption(),
	}
}
