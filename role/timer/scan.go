package timer

import (
	"github.com/zywaited/delay-queue/role"
	"github.com/zywaited/delay-queue/role/task"
)

type ScannerType string

const DefaultScannerTypeName ScannerType = "default-timer-scanner"

type Scanner interface {
	role.Launcher

	Add(task.Task) error

	Remove(task.Task) error

	Name() ScannerType
}

var scanners map[ScannerType]Scanner

func init() {
	scanners = make(map[ScannerType]Scanner)
}

func RegisterScanner(st ScannerType, sr Scanner) {
	scanners[st] = sr
}

func AcScanner(st ScannerType) Scanner {
	if scanners[st] != nil {
		return scanners[st]
	}
	if scanners[DefaultScannerTypeName] != nil {
		return scanners[DefaultScannerTypeName]
	}
	for _, scanner := range scanners {
		return scanner
	}
	return nil
}
