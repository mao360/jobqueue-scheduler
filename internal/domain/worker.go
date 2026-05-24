package domain

type WorkerID string

type Worker struct {
	ID                 WorkerID
	SupportedTaskTypes []string
	MaxConcurrentJobs  int
}