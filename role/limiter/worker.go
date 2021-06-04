package limiter

import "time"

type workers []Worker

func (ws *workers) len() int {
	return len(*ws)
}

func (ws *workers) acquire() (w Worker) {
	if ws.len() == 0 {
		return
	}
	w = (*ws)[0]
	*ws = (*ws)[1:]
	return
}

func defaultWorkerCreator() Worker {
	return nil
}

type Worker interface {
	Run(TaskJob)
	Expire(time.Duration) bool
}

type poolWorker struct {
	w     Worker
	tasks chan TaskJob
	last  time.Time
}

func newPoolWorker(w Worker) *poolWorker {
	return &poolWorker{w: w, tasks: make(chan TaskJob)}
}

func (pw *poolWorker) Run(task TaskJob) {
	if pw.w == nil {
		task()
		return
	}
	pw.w.Run(task)
}

func (pw *poolWorker) Expire(t time.Duration) bool {
	return time.Now().Sub(pw.last) > t || (pw.w != nil && pw.w.Expire(t))
}
