package connectrpc

import (
	"context"
	"errors"
	"io"
	"sync"

	"connectrpc.com/connect"
	v1 "github.com/mao360/jobqueue-proto/gen/go/jobqueue/v1"
	"github.com/mao360/jobqueue-proto/gen/go/jobqueue/v1/jobqueuev1connect"
	"github.com/mao360/jobqueue-scheduler/internal/domain"
	"github.com/mao360/jobqueue-scheduler/internal/gateway"
)

type WorkerRegistry interface {
	Register(info domain.Worker, s gateway.WorkerSender)
	Unregister(id domain.WorkerID)
	Release(id domain.WorkerID)
}

type WorkerUsecase interface {
	StartJob(ctx context.Context, id domain.JobID, workerID domain.WorkerID) error
	FinishJob(ctx context.Context, id domain.JobID, status domain.JobStatus, result []byte, errMsg string) error
	ReportEvent(event domain.JobEvent)
}

type WorkerHandler struct {
	jobqueuev1connect.UnimplementedWorkerGatewayServiceHandler
	registry WorkerRegistry
	uc       WorkerUsecase
}

func NewWorkerHandler(registry WorkerRegistry, uc WorkerUsecase) *WorkerHandler {
	return &WorkerHandler{registry: registry, uc: uc}
}

func (h *WorkerHandler) Connect(ctx context.Context, stream *connect.BidiStream[v1.ConnectRequest, v1.ConnectResponse]) error {
	first, err := stream.Receive()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}

	hello := first.GetHello()
	if hello == nil {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("first message must be hello"))
	}

	workerID := domain.WorkerID(hello.WorkerId)
	info := domain.Worker{
		ID:                 workerID,
		SupportedTaskTypes: hello.SupportedTaskTypes,
		MaxConcurrentJobs:  int(hello.MaxConcurrentJobs),
	}

	sender := &workerStream{stream: stream}
	
	if err := stream.Send(&v1.ConnectResponse{
		Message: &v1.ConnectResponse_Welcome{
			Welcome: &v1.SchedulerWelcome{AssignedWorkerId: string(workerID)},
		},
	}); err != nil {
		return err
	}

	h.registry.Register(info, sender)
	defer h.registry.Unregister(workerID)

	for {
		req, err := stream.Receive()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		switch req.Message.(type) {
		case *v1.ConnectRequest_JobAccepted:
			ja := req.GetJobAccepted()
			if err := h.uc.StartJob(ctx, domain.JobID(ja.JobId), workerID); err != nil {
				return toConnectErr(err)
			}
		case *v1.ConnectRequest_JobEvent:
			h.uc.ReportEvent(protoEventToDomain(req.GetJobEvent()))
		case *v1.ConnectRequest_JobResult:
			jr := req.GetJobResult()
			h.registry.Release(workerID)
			if err := h.uc.FinishJob(ctx, domain.JobID(jr.JobId), protoJobStatusToDomain(jr.FinalStatus), jr.Result, jr.Error); err != nil {
				return toConnectErr(err)
			}
		case *v1.ConnectRequest_Heartbeat:
		}
	}
}

type workerStream struct {
	mu     sync.Mutex
	stream *connect.BidiStream[v1.ConnectRequest, v1.ConnectResponse]
}

func (w *workerStream) Send(ctx context.Context, job *domain.Job) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.stream.Send(&v1.ConnectResponse{
		Message: &v1.ConnectResponse_JobAssignment{
			JobAssignment: &v1.JobAssignment{Job: domainJobToProto(*job)},
		},
	})
}