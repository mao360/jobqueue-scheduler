// Package integration — интеграционные тесты scheduler'а: настоящий HTTP-сервер
// (httptest, in-process), настоящая сериализация protobuf, настоящие стримы.
// Воркеры эмулируются фейком из fakeworker_test.go.
package integration

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	"connectrpc.com/validate"

	"github.com/mao360/jobqueue-proto/gen/go/jobqueue/v1/jobqueuev1connect"
	"github.com/mao360/jobqueue-scheduler/internal/eventbus"
	"github.com/mao360/jobqueue-scheduler/internal/gateway"
	"github.com/mao360/jobqueue-scheduler/internal/repository/memory"
	"github.com/mao360/jobqueue-scheduler/internal/transport/connectrpc"
	"github.com/mao360/jobqueue-scheduler/internal/usecase"
)

// env — поднятый на время теста scheduler и клиенты к обоим его сервисам.
type env struct {
	Scheduler jobqueuev1connect.SchedulerServiceClient
	Gateway   jobqueuev1connect.WorkerGatewayServiceClient
}

// newEnv собирает scheduler так же, как internal/app (та же проводка,
// тот же validate-интерсептор), но поверх httptest вместо ListenAndServe.
// EnableHTTP2 обязателен: без h2 bidi-стрим воркера не заработает.
func newEnv(t *testing.T) *env {
	t.Helper()

	repo := memory.New()
	bus := eventbus.New()
	gw := gateway.New()

	uc := usecase.New(repo, bus, gw, bus)
	h := connectrpc.NewGRPCHandler(uc)
	wh := connectrpc.NewWorkerHandler(gw, uc)

	interceptors := connect.WithInterceptors(validate.NewInterceptor())

	mux := http.NewServeMux()
	mux.Handle(jobqueuev1connect.NewSchedulerServiceHandler(h, interceptors))
	mux.Handle(jobqueuev1connect.NewWorkerGatewayServiceHandler(wh, interceptors))

	srv := httptest.NewUnstartedServer(mux)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	return &env{
		Scheduler: jobqueuev1connect.NewSchedulerServiceClient(srv.Client(), srv.URL),
		Gateway:   jobqueuev1connect.NewWorkerGatewayServiceClient(srv.Client(), srv.URL),
	}
}
