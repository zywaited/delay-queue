package server

type InitOption interface {
	Run(*DelayQueue) error
	Stop(*DelayQueue) error
}

func AcquireServerInits() []InitOption {
	return []InitOption{
		NewConfigInitOption(),
		NewBaseLevelOption(),
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
