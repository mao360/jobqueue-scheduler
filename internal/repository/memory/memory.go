package memory

import (
	"context"
	"sync"

	"github.com/mao360/jobqueue-scheduler/internal/domain"
)

type Memory struct {
	mu   sync.RWMutex
	jobs map[domain.JobID]*domain.Job
}

func New() *Memory {
	return &Memory{
		jobs: make(map[domain.JobID]*domain.Job),
	}
}

func (m *Memory) Create(ctx context.Context, job *domain.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.jobs[job.ID]; ok {
		return domain.ErrJobAlreadyExists
	}
	j := *job
	m.jobs[job.ID] = &j

	return nil
}

func (m *Memory) Get(ctx context.Context, jobID domain.JobID) (*domain.Job, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	job, ok := m.jobs[jobID]
	if !ok {
		return nil, domain.ErrJobNotFound
	}

	j := *job
	return &j, nil
}

func (m *Memory) Update(ctx context.Context, job *domain.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.jobs[job.ID]; !ok {
		return domain.ErrJobNotFound
	}

	j := *job
	m.jobs[job.ID] = &j

	return nil
}
