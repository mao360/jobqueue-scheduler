package domain

import "fmt"

type JobStatus uint8

const (
	StatusUnspecified JobStatus = iota
	StatusPending
	StatusRunning
	StatusSucceeded
	StatusFailed
	StatusCancelled
)

func (s JobStatus) String() string {
	switch s {
	case StatusPending:
		return "pending"
	case StatusRunning:
		return "running"
	case StatusSucceeded:
		return "succeeded"
	case StatusFailed:
		return "failed"
	case StatusCancelled:
		return "cancelled"
	default:
		return fmt.Sprintf("unspecified(%d)", uint8(s))
	}
}

func (s JobStatus) IsTerminal() bool {
	switch s {
	case StatusSucceeded, StatusFailed, StatusCancelled:
		return true
	default:
		return false
	}
}

func (s JobStatus) CanTransitionTo(next JobStatus) bool {
	switch s {
	case StatusPending:
		return next == StatusRunning || next == StatusCancelled
	case StatusRunning:
		return next == StatusSucceeded || next == StatusFailed || next == StatusCancelled
	default:
		return false
	}
}