package connectrpc

import (
	"errors"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/mao360/jobqueue-proto/gen/go/jobqueue/v1"
	"github.com/mao360/jobqueue-scheduler/internal/domain"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func toConnectErr(err error) error {
	switch {
	case errors.Is(err, domain.ErrJobNotFound):
		return connect.NewError(connect.CodeNotFound, err)
	case errors.Is(err, domain.ErrInvalidTransition):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	case errors.Is(err, domain.ErrInvalidJob):
		return connect.NewError(connect.CodeInvalidArgument, err)
	case errors.Is(err, domain.ErrJobAlreadyDone):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	case errors.Is(err, domain.ErrJobAlreadyExists):
		return connect.NewError(connect.CodeAlreadyExists, err)
	default:
		return connect.NewError(connect.CodeInternal, errors.New("internal error"))
	}
}

func domainJobToProto(job domain.Job) *v1.Job {
	return &v1.Job{
		Id:               string(job.ID),
		TaskType:         job.TaskType,
		Payload:          job.Payload,
		Status:           domainJobStatusToProto(job.Status),
		TimeoutSeconds:   int32(job.Timeout / time.Second),
		Result:           job.Result,
		Error:            job.Error,
		AssignedWorkerId: string(job.AssignedWorkerID),
		CreatedAt:        tsOrNil(job.CreatedAt),
		StartedAt:        tsOrNil(job.StartedAt),
		FinishedAt:       tsOrNil(job.FinishedAt),
	}
}

func domainJobStatusToProto(jobStatus domain.JobStatus) v1.JobStatus {
	switch jobStatus {
	case domain.StatusPending:
		return v1.JobStatus_JOB_STATUS_PENDING
	case domain.StatusRunning:
		return v1.JobStatus_JOB_STATUS_RUNNING
	case domain.StatusSucceeded:
		return v1.JobStatus_JOB_STATUS_SUCCEEDED
	case domain.StatusFailed:
		return v1.JobStatus_JOB_STATUS_FAILED
	case domain.StatusCancelled:
		return v1.JobStatus_JOB_STATUS_CANCELLED
	default:
		return v1.JobStatus_JOB_STATUS_UNSPECIFIED
	}
}

func domainEventToProto(ev domain.JobEvent) *v1.JobEvent {
	pe := &v1.JobEvent{
		JobId: string(ev.JobID),
		At:    tsOrNil(ev.OccurredAt),
	}

	switch p := ev.Payload.(type) {
	case domain.StatusChanged:
		pe.Event = &v1.JobEvent_StatusChanged{StatusChanged: &v1.JobStatusChanged{
			OldStatus: domainJobStatusToProto(p.From),
			NewStatus: domainJobStatusToProto(p.To),
		}}
	case domain.Progress:
		pe.Event = &v1.JobEvent_Progress{Progress: &v1.JobProgress{
			Percent: int32(p.Percent),
			Message: p.Message,
		}}
	case domain.Log:
		pe.Event = &v1.JobEvent_Log{Log: &v1.JobLog{Line: p.Line}}
	}

	return pe
}

func protoJobStatusToDomain(s v1.JobStatus) domain.JobStatus {
	switch s {
	case v1.JobStatus_JOB_STATUS_PENDING:
		return domain.StatusPending
	case v1.JobStatus_JOB_STATUS_RUNNING:
		return domain.StatusRunning
	case v1.JobStatus_JOB_STATUS_SUCCEEDED:
		return domain.StatusSucceeded
	case v1.JobStatus_JOB_STATUS_FAILED:
		return domain.StatusFailed
	case v1.JobStatus_JOB_STATUS_CANCELLED:
		return domain.StatusCancelled
	default:
		return domain.StatusUnspecified
	}
}

func protoEventToDomain(e *v1.JobEvent) domain.JobEvent {
	ev := domain.JobEvent{
		JobID:      domain.JobID(e.JobId),
		OccurredAt: tsToTime(e.At),
	}

	switch m := e.Event.(type) {
	case *v1.JobEvent_StatusChanged:
		ev.Payload = domain.StatusChanged{
			From: protoJobStatusToDomain(m.StatusChanged.OldStatus),
			To:   protoJobStatusToDomain(m.StatusChanged.NewStatus),
		}
	case *v1.JobEvent_Progress:
		ev.Payload = domain.Progress{
			Percent: int(m.Progress.Percent),
			Message: m.Progress.Message,
		}
	case *v1.JobEvent_Log:
		ev.Payload = domain.Log{Line: m.Log.Line}
	}

	return ev
}

func tsToTime(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Now()
	}
	return ts.AsTime()
}

func tsOrNil(ts time.Time) *timestamppb.Timestamp {
	if ts.IsZero() {
		return nil
	}
	return timestamppb.New(ts)
}
