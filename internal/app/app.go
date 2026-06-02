package app

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/mao360/jobqueue-proto/gen/go/jobqueue/v1/jobqueuev1connect"
	"github.com/mao360/jobqueue-scheduler/internal/repository/memory"
	"github.com/mao360/jobqueue-scheduler/internal/transport/connectrpc"
	"github.com/mao360/jobqueue-scheduler/internal/usecase"
)

type App struct {
	srv *http.Server
}

func New() *App {
	m := memory.New()
	uc := usecase.New(m)
	h := connectrpc.NewGRPCHandler(uc)

	mux := http.NewServeMux()
	mux.Handle(jobqueuev1connect.NewSchedulerServiceHandler(h))

	proto := new(http.Protocols)
	proto.SetHTTP1(true)
	proto.SetUnencryptedHTTP2(true)

	srv := &http.Server{
		Addr:      ":8080",
		Handler:   mux,
		Protocols: proto,
	}

	return &App{
		srv: srv,
	}

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
		shCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		return a.srv.Shutdown(shCtx)
	case err := <-errCh:
		return err
	}
}
