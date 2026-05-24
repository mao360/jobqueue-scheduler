package domain

import "time"

type JobID string

type Job struct {
	ID               JobID
	TaskType         string
	Payload          []byte
	Status           JobStatus
	Timeout          time.Duration
	Result           []byte
	Error            string
	AssignedWorkerID WorkerID
	CreatedAt        time.Time
	StartedAt        time.Time
	FinishedAt       time.Time
}

func NewJob(id JobID, taskType string, payload []byte, timeout time.Duration, now time.Time) (*Job, error) {
	if id == "" {
		return nil, ErrInvalidJob
	}
	if taskType == "" {
		return nil, ErrInvalidJob
	}
	if timeout <= 0 {
		return nil, ErrInvalidJob
	}
	return &Job{
		ID:        id,
		TaskType:  taskType,
		Payload:   payload,
		Status:    StatusPending,
		Timeout:   timeout,
		CreatedAt: now,
	}, nil
}

func (j *Job) MarkRunning(at time.Time, workerID WorkerID) error {
	if !j.Status.CanTransitionTo(StatusRunning) {
		return ErrInvalidTransition
	}
	j.Status = StatusRunning
	j.StartedAt = at
	j.AssignedWorkerID = workerID
	return nil
}

func (j *Job) MarkSucceeded(at time.Time, result []byte) error {
	if !j.Status.CanTransitionTo(StatusSucceeded) {
		return ErrInvalidTransition
	}
	j.Status = StatusSucceeded
	j.FinishedAt = at
	j.Result = result
	return nil
}

func (j *Job) MarkFailed(at time.Time, errMsg string) error {
	if !j.Status.CanTransitionTo(StatusFailed) {
		return ErrInvalidTransition
	}
	j.Status = StatusFailed
	j.FinishedAt = at
	j.Error = errMsg
	return nil
}

func (j *Job) MarkCancelled(at time.Time) error {
	if !j.Status.CanTransitionTo(StatusCancelled) {
		return ErrInvalidTransition
	}
	j.Status = StatusCancelled
	j.FinishedAt = at
	return nil
}