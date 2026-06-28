package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/mao360/jobqueue-scheduler/internal/app"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	application, err := app.New()
	if err != nil {
		log.Fatalf("can`t init: %s", err.Error())
	}

	if err := application.Run(ctx); err != nil {
		log.Fatalf("can`t start: %s", err.Error())
	}
}
