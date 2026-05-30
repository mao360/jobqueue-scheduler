package connectrpc

import (
	"context"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/mao360/jobqueue-proto/gen/go/jobqueue/v1"
	"github.com/mao360/jobqueue-proto/gen/go/jobqueue/v1/jobqueuev1connect"
	"github.com/mao360/jobqueue-scheduler/internal/usecase"
)

type grpcHandler struct {
	jobqueuev1connect.UnimplementedSchedulerServiceHandler
	uc *usecase.JobUsecase
}

func NewGRPCHandler(uc *usecase.JobUsecase) *grpcHandler {
	return &grpcHandler{uc: uc}
}

func (gh *grpcHandler) Get(ctx context.Context, req *connect.Request[v1.CreateJobRequest]) (*connect.Response[v1.CreateJobResponse], error) {
	m := req.Msg
	id, err := h.uc.CreateJob(ctx, m.TaskType, m.Payload, time.Duration(m.TimeoutSeconds)*time.Second)
	if err != nil {
		return nil, toConnectError(err)
	}
	job, err := h.uc.GetJob(ctx, id) // чтобы вернуть полный Job
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&v1.CreateJobResponse{Job: domainJobToProto(job)}), nil
}
