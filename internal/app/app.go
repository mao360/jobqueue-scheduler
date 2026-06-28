package app

import (
	"context"
	"errors"
	"net/http"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/validate"
	"github.com/mao360/jobqueue-proto/gen/go/jobqueue/v1/jobqueuev1connect"
	"github.com/mao360/jobqueue-scheduler/internal/config"
	"github.com/mao360/jobqueue-scheduler/internal/eventbus"
	"github.com/mao360/jobqueue-scheduler/internal/gateway"
	"github.com/mao360/jobqueue-scheduler/internal/repository/memory"
	"github.com/mao360/jobqueue-scheduler/internal/transport/connectrpc"
	"github.com/mao360/jobqueue-scheduler/internal/usecase"
)

type App struct {
	srv             *http.Server
	shutdownTimeout time.Duration
}

func New() (*App, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, err
	}

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

	proto := new(http.Protocols)
	proto.SetHTTP1(true)
	proto.SetUnencryptedHTTP2(true)

	srv := &http.Server{
		Addr:      cfg.ListenAddr,
		Handler:   mux,
		Protocols: proto,
	}

	return &App{
		srv:             srv,
		shutdownTimeout: cfg.ShutdownTimeout,
	}, nil
}

func (a *App) Run(ctx context.Context) error {
	errCh := make(chan error, 1)

	go func() {
		err := a.srv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		shCtx, cancel := context.WithTimeout(context.Background(), a.shutdownTimeout)
		defer cancel()
		return a.srv.Shutdown(shCtx)
	case err := <-errCh:
		return err
	}
}