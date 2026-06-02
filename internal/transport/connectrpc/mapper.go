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

func tsOrNil(ts time.Time) *timestamppb.Timestamp {
	if ts.IsZero() {
		return nil
	}
	return timestamppb.New(ts)
}
