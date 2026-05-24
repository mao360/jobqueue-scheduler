package domain

import "time"

type EventPayload interface {
	isEventPayload()
}

type StatusChanged struct {
	From JobStatus
	To   JobStatus
}

type Progress struct {
	Percent int
	Message string
}

type Log struct {
	Line string
}

func (StatusChanged) isEventPayload() {}
func (Progress) isEventPayload()      {}
func (Log) isEventPayload()           {}

var (
	_ EventPayload = StatusChanged{}
	_ EventPayload = Progress{}
	_ EventPayload = Log{}
)

type JobEvent struct {
	JobID      JobID
	OccurredAt time.Time
	Payload    EventPayload
}
