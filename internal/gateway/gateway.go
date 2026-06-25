package gateway

import (
	"context"
	"sync"

	"github.com/mao360/jobqueue-scheduler/internal/domain"
)

type WorkerSender interface {
	Send(ctx context.Context, job *domain.Job) error
}

type worker struct {
	info   domain.Worker
	sender WorkerSender
	active int
}

type Gateway struct {
	mu      sync.Mutex
	workers map[domain.WorkerID]*worker
}

func New() *Gateway {
	return &Gateway{
		workers: make(map[domain.WorkerID]*worker),
	}
}

func (g *Gateway) Register(info domain.Worker, s WorkerSender) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.workers[info.ID] = &worker{info, s, 0}
}

func (g *Gateway) Unregister(id domain.WorkerID) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.workers, id)
}

func (g *Gateway) Dispatch(ctx context.Context, job *domain.Job) error {
	g.mu.Lock()
	var (
		chosen   *worker
		chosenID domain.WorkerID
	)
	for id, w := range g.workers {
		if w.active < w.info.MaxConcurrentJobs && taskTypeIn(job.TaskType, w.info.SupportedTaskTypes) {
			w.active++
			chosen, chosenID = w, id
			break
		}
	}
	g.mu.Unlock()

	if chosen == nil {
		return domain.ErrNoWorker
	}
	
	if err := chosen.sender.Send(ctx, job); err != nil {
		g.Release(chosenID)
		return err
	}
	return nil
}

func (g *Gateway) Release(id domain.WorkerID) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if w, ok := g.workers[id]; ok && w.active > 0 {
		w.active--
	}
}

func taskTypeIn(taskType string, supportedTaskTypes []string) bool {
	for _, t := range supportedTaskTypes {
		if t == taskType {
			return true
		}
	}
	return false
}
