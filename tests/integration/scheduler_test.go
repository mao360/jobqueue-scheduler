package integration

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	v1 "github.com/mao360/jobqueue-proto/gen/go/jobqueue/v1"
)

// requireRecv читает из канала с таймаутом. Единственное допустимое
// «ожидание» в тестах — никаких time.Sleep.
func requireRecv[T any](t *testing.T, ch <-chan T, timeout time.Duration) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(timeout):
		t.Fatalf("timed out after %s waiting for message", timeout)
		panic("unreachable")
	}
}

// Образцовый happy path: клиент создаёт задачу -> scheduler диспетчит её
// воркеру -> воркер принимает и завершает -> клиент видит SUCCEEDED.
func TestCreateJob_HappyPath(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := context.Background()

	worker := startFakeWorker(t, e.Gateway, "worker-1", 1, "echo")

	created, err := e.Scheduler.CreateJob(ctx, connect.NewRequest(&v1.CreateJobRequest{
		TaskType:       "echo",
		Payload:        []byte("ping"),
		TimeoutSeconds: 5,
	}))
	require.NoError(t, err)
	jobID := created.Msg.GetJob().GetId()

	assigned := requireRecv(t, worker.Assignments, time.Second)
	require.Equal(t, jobID, assigned.GetId())

	worker.Accept(jobID)
	worker.Finish(jobID, v1.JobStatus_JOB_STATUS_SUCCEEDED, []byte("ping"), "")

	// Терминальный статус доезжает асинхронно (receive-loop гейтвея),
	// поэтому опрашиваем GetJob, а не проверяем сразу.
	require.Eventually(t, func() bool {
		got, err := e.Scheduler.GetJob(ctx, connect.NewRequest(&v1.GetJobRequest{JobId: jobID}))
		return err == nil && got.Msg.GetJob().GetStatus() == v1.JobStatus_JOB_STATUS_SUCCEEDED
	}, 2*time.Second, 10*time.Millisecond)
}

func TestCreateJob_NoWorker_StaysPending(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := context.Background()

	created, err := e.Scheduler.CreateJob(ctx, connect.NewRequest(&v1.CreateJobRequest{
		TaskType:       "echo",
		Payload:        []byte("ping"),
		TimeoutSeconds: 5,
	}))
	require.NoError(t, err)
	require.Equal(t, v1.JobStatus_JOB_STATUS_PENDING, created.Msg.GetJob().GetStatus())

	got, err := e.Scheduler.GetJob(ctx, connect.NewRequest(&v1.GetJobRequest{
		JobId: created.Msg.GetJob().GetId(),
	}))
	require.NoError(t, err)
	require.Equal(t, v1.JobStatus_JOB_STATUS_PENDING, got.Msg.GetJob().GetStatus())
}

func TestCreateJob_ValidationRejected(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := context.Background()
	_, err := e.Scheduler.CreateJob(ctx, connect.NewRequest(&v1.CreateJobRequest{
		TaskType:       "",
		Payload:        []byte("ping"),
		TimeoutSeconds: 5,
	}))

	var cErr *connect.Error
	require.ErrorAs(t, err, &cErr)
	require.Equal(t, connect.CodeInvalidArgument, cErr.Code())
}

func TestSubmitBatch_ClientStreaming(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := context.Background()
	
	reqs := []*v1.CreateJobRequest{
		{TaskType: "echo", Payload: []byte("a"), TimeoutSeconds: 5},
		{TaskType: "echo", Payload: []byte("b"), TimeoutSeconds: 0}, // rejected
		{TaskType: "echo", Payload: []byte("c"), TimeoutSeconds: 5},
	}

	stream := e.Scheduler.SubmitBatch(ctx)
	for _, r := range reqs {
		require.NoError(t, stream.Send(&v1.SubmitBatchRequest{Job: r}))
	}

	res, err := stream.CloseAndReceive()
	require.NoError(t, err)
	require.Equal(t, int32(2), res.Msg.GetAcceptedCount())
	require.Equal(t, int32(1), res.Msg.GetRejectedCount())
	require.Len(t, res.Msg.GetJobs(), 2)
}