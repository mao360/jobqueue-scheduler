package connectrpc

import (
	"context"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/mao360/jobqueue-proto/gen/go/jobqueue/v1"
	"github.com/mao360/jobqueue-proto/gen/go/jobqueue/v1/jobqueuev1connect"
	"github.com/mao360/jobqueue-scheduler/internal/domain"
)

type Usecase interface {
	CreateJob(ctx context.Context, taskType string, payload []byte, timeout time.Duration) (*domain.Job, error)
	GetJob(ctx context.Context, id domain.JobID) (*domain.Job, error)
	CancelJob(ctx context.Context, id domain.JobID) error
}
type GRPCHandler struct {
	jobqueuev1connect.UnimplementedSchedulerServiceHandler
	uc Usecase
}

func NewGRPCHandler(uc Usecase) *GRPCHandler {
	return &GRPCHandler{uc: uc}
}

func (gh *GRPCHandler) CreateJob(ctx context.Context, req *connect.Request[v1.CreateJobRequest]) (*connect.Response[v1.CreateJobResponse], error) {
	timeout := time.Duration(req.Msg.TimeoutSeconds) * time.Second
	job, err := gh.uc.CreateJob(ctx, req.Msg.TaskType, req.Msg.Payload, timeout)
	if err != nil {
		return nil, toConnectErr(err)
	}

	return connect.NewResponse(&v1.CreateJobResponse{Job: domainJobToProto(*job)}), nil
}

func (gh *GRPCHandler) GetJob(ctx context.Context, req *connect.Request[v1.GetJobRequest]) (*connect.Response[v1.GetJobResponse], error) {
	job, err := gh.uc.GetJob(ctx, domain.JobID(req.Msg.JobId))
	if err != nil {
		return nil, toConnectErr(err)
	}

	return connect.NewResponse(&v1.GetJobResponse{Job: domainJobToProto(*job)}), nil
}

func (gh *GRPCHandler) CancelJob(ctx context.Context, req *connect.Request[v1.CancelJobRequest]) (*connect.Response[v1.CancelJobResponse], error) {
	if err := gh.uc.CancelJob(ctx, domain.JobID(req.Msg.JobId)); err != nil {
		return nil, toConnectErr(err)
	}
	return connect.NewResponse(&v1.CancelJobResponse{}), nil
}
