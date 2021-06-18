package service

import (
	"time"

	"github.com/zywaited/delay-queue/parser/system"
	"github.com/zywaited/delay-queue/role"
	"github.com/zywaited/delay-queue/role/task"
	"github.com/zywaited/delay-queue/role/timer"
	"github.com/zywaited/delay-queue/transport"
	"github.com/zywaited/go-common/limiter"
	"github.com/zywaited/go-common/xcopy"
)

type HandleOption func(*Handle)

func HandleOptionWithStore(store role.DataStore) HandleOption {
	return func(h *Handle) {
		h.store = store
	}
}

func HandleOptionWithTimer(timer timer.Scanner) HandleOption {
	return func(h *Handle) {
		h.timer = timer
	}
}

func HandleOptionWithTp(tp task.Factory) HandleOption {
	return func(h *Handle) {
		h.tp = tp
	}
}

func HandleOptionWithTimeOut(to time.Duration) HandleOption {
	return func(h *Handle) {
		h.timeout = to
	}
}

func HandleOptionWithRunner(runner task.Runner) HandleOption {
	return func(h *Handle) {
		h.runner = runner
	}
}

func HandleOptionWithBaseTime(base time.Duration) HandleOption {
	return func(h *Handle) {
		h.baseTime = base
	}
}

func HandleOptionWithLogger(logger system.Logger) HandleOption {
	return func(h *Handle) {
		h.logger = logger
	}
}

func HandleOptionWithXCopy(c xcopy.XCopy) HandleOption {
	return func(h *Handle) {
		h.cp = c
	}
}

func HandleOptionTransporters(ts transport.TransporterM) HandleOption {
	return func(h *Handle) {
		for t, tr := range ts {
			h.ts[t] = tr
		}
	}
}

func HandleOptionWithWait(wait bool) HandleOption {
	return func(h *Handle) {
		h.wait = wait
	}
}

func HandleOptionWithGP(gp limiter.Pool) HandleOption {
	return func(h *Handle) {
		h.gp = gp
	}
}

func HandleOptionWithIdCreator(version role.GenerateIds) HandleOption {
	return func(h *Handle) {
		h.idCreator = version
	}
}
