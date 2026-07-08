package integration

import (
	"context"
	"sync"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	v1 "github.com/mao360/jobqueue-proto/gen/go/jobqueue/v1"
	"github.com/mao360/jobqueue-proto/gen/go/jobqueue/v1/jobqueuev1connect"
)

// fakeWorker — честная минимальная реализация протокола воркера поверх
// bidi-стрима. Не мок: он реально ходит по сети (in-process httptest) и
// соблюдает порядок hello -> welcome -> assignments. Сценарий поведения
// («принять и завершить», «принять и умереть») задаётся в самом тесте
// через методы Accept/Finish/Close — так один фейк покрывает все кейсы.
type fakeWorker struct {
	t         *testing.T
	stream    *connect.BidiStreamForClient[v1.ConnectRequest, v1.ConnectResponse]
	cancel    context.CancelFunc
	closeOnce sync.Once

	// Assignments — задачи, назначенные этому воркеру scheduler'ом.
	// Тест читает отсюда с таймаутом (см. requireRecv в scheduler_test.go).
	Assignments chan *v1.Job
	// Cancels — job_id, для которых пришла CancelJobInstruction.
	Cancels chan string
}

// startFakeWorker подключается к gateway, представляется и запускает
// receive-loop. Клиентская половина того же рукопожатия, что серверная
// в transport/connectrpc/worker.go.
func startFakeWorker(t *testing.T, gw jobqueuev1connect.WorkerGatewayServiceClient, id string, maxJobs int32, taskTypes ...string) *fakeWorker {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	stream := gw.Connect(ctx)

	w := &fakeWorker{
		t:           t,
		stream:      stream,
		cancel:      cancel,
		Assignments: make(chan *v1.Job, 16),
		Cancels:     make(chan string, 16),
	}
	t.Cleanup(w.Close)

	require.NoError(t, stream.Send(&v1.ConnectRequest{
		Message: &v1.ConnectRequest_Hello{
			Hello: &v1.WorkerHello{
				WorkerId:           id,
				SupportedTaskTypes: taskTypes,
				MaxConcurrentJobs:  maxJobs,
			},
		},
	}))

	welcome, err := stream.Receive()
	require.NoError(t, err)
	require.NotNil(t, welcome.GetWelcome(), "first message from scheduler must be welcome")

	go w.receiveLoop()

	return w
}

func (w *fakeWorker) receiveLoop() {
	for {
		msg, err := w.stream.Receive()
		if err != nil {
			return // стрим закрыт (Close теста или падение сервера) — выходим молча
		}
		switch m := msg.Message.(type) {
		case *v1.ConnectResponse_JobAssignment:
			w.Assignments <- m.JobAssignment.GetJob()
		case *v1.ConnectResponse_Cancel:
			w.Cancels <- m.Cancel.GetJobId()
		case *v1.ConnectResponse_Heartbeat:
		}
	}
}

// Accept сообщает scheduler'у, что задача взята в работу (PENDING -> RUNNING).
func (w *fakeWorker) Accept(jobID string) {
	w.t.Helper()
	require.NoError(w.t, w.stream.Send(&v1.ConnectRequest{
		Message: &v1.ConnectRequest_JobAccepted{
			JobAccepted: &v1.JobAccepted{JobId: jobID},
		},
	}))
}

// Finish завершает задачу с финальным статусом (RUNNING -> терминальный).
func (w *fakeWorker) Finish(jobID string, status v1.JobStatus, result []byte, errMsg string) {
	w.t.Helper()
	require.NoError(w.t, w.stream.Send(&v1.ConnectRequest{
		Message: &v1.ConnectRequest_JobResult{
			JobResult: &v1.JobResult{
				JobId:       jobID,
				FinalStatus: status,
				Result:      result,
				Error:       errMsg,
			},
		},
	}))
}

func (w *fakeWorker) Progress(jobID string, percent int32, message string) {
	w.t.Helper()
	require.NoError(w.t, w.stream.Send(&v1.ConnectRequest{
		Message: &v1.ConnectRequest_JobEvent{
			JobEvent: &v1.JobEvent{
				JobId: jobID,
				Event: &v1.JobEvent_Progress{
					Progress: &v1.JobProgress{Percent: percent, Message: message},
				},
			},
		},
	}))
}

func (w *fakeWorker) Close() {
	w.closeOnce.Do(func() {
		_ = w.stream.CloseRequest()
		_ = w.stream.CloseResponse()
		w.cancel()
	})
}