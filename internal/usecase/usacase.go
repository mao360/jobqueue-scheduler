package usecase

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/mao360/jobqueue-scheduler/internal/domain"
)

type JobRepository interface {
	Create(ctx context.Context, job *domain.Job) error
	Get(ctx context.Context, id domain.JobID) (*domain.Job, error)
	Update(ctx context.Context, job *domain.Job) error
}

type JobUsecase struct {
	repo JobRepository
}

func New(repo JobRepository) *JobUsecase {
	return &JobUsecase{repo: repo}
}

func (u *JobUsecase) CreateJob(ctx context.Context, taskType string, payload []byte, timeout time.Duration) (domain.JobID, error) {
	id := domain.JobID(uuid.NewString())

	job, err := domain.NewJob(id, taskType, payload, timeout, time.Now())
	if err != nil {
		return "", err
	}

	if err := u.repo.Create(ctx, job); err != nil {
		return "", err
	}

	return job.ID, nil
}

func (u *JobUsecase) GetJob(ctx context.Context, id domain.JobID) (*domain.Job, error) {
	return u.repo.Get(ctx, id)
}

func (u *JobUsecase) CancelJob(ctx context.Context, id domain.JobID) error {
	job, err := u.repo.Get(ctx, id)
	if err != nil {
		return err
	}

	if err = job.MarkCancelled(time.Now()); err != nil {
		return err
	}

	if err = u.repo.Update(ctx, job); err != nil {
		return err
	}
	return nil
}
