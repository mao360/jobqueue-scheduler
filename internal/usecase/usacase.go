package usecase

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/mao360/jobqueue-scheduler/internal/domain"
)

type JobRepository interface {
	Create(ctx context.Context, job *domain.Job) error
	Get(ctx context.Context, id domain.JobID) (*domain.Job, error)
	Update(ctx context.Context, job *domain.Job) error
}

type EventPublisher interface {
	Publish(event domain.JobEvent)
}

type WorkerSender interface {
	Dispatch(ctx context.Context, job *domain.Job) error
}

type EventSubscriber interface {
	Subscribe(jobID domain.JobID) (<-chan domain.JobEvent, func())
}

type JobUsecase struct {
	repo            JobRepository
	publisher       EventPublisher
	workerSender    WorkerSender
	eventSubscriber EventSubscriber
}

func New(repo JobRepository, publisher EventPublisher, sender WorkerSender, eventSubscriber EventSubscriber) *JobUsecase {
	return &JobUsecase{
		repo:            repo,
		publisher:       publisher,
		workerSender:    sender,
		eventSubscriber: eventSubscriber,
	}
}

func (u *JobUsecase) CreateJob(ctx context.Context, taskType string, payload []byte, timeout time.Duration) (*domain.Job, error) {
	id := domain.JobID(uuid.NewString())

	job, err := domain.NewJob(id, taskType, payload, timeout, time.Now())
	if err != nil {
		return nil, err
	}

	if err = u.repo.Create(ctx, job); err != nil {
		return nil, err
	}

	if err = u.workerSender.Dispatch(ctx, job); err != nil {
		if errors.Is(err, domain.ErrNoWorker) {
			return job, nil
		}
		return nil, err
	}

	return job, nil
}

func (u *JobUsecase) GetJob(ctx context.Context, id domain.JobID) (*domain.Job, error) {
	return u.repo.Get(ctx, id)
}

func (u *JobUsecase) CancelJob(ctx context.Context, id domain.JobID) error {
	job, err := u.repo.Get(ctx, id)
	if err != nil {
		return err
	}

	from := job.Status
	if err = job.MarkCancelled(time.Now()); err != nil {
		return err
	}

	if err = u.repo.Update(ctx, job); err != nil {
		return err
	}

	u.publishStatus(id, from, job.Status)
	return nil
}

func (u *JobUsecase) WatchJob(id domain.JobID) (<-chan domain.JobEvent, func()) {
	return u.eventSubscriber.Subscribe(id)
}


func (u *JobUsecase) StartJob(ctx context.Context, id domain.JobID, workerID domain.WorkerID) error {
	job, err := u.repo.Get(ctx, id)
	if err != nil {
		return err
	}

	from := job.Status
	if err = job.MarkRunning(time.Now(), workerID); err != nil {
		return err
	}

	if err = u.repo.Update(ctx, job); err != nil {
		return err
	}

	u.publishStatus(id, from, job.Status)
	return nil
}

func (u *JobUsecase) FinishJob(ctx context.Context, id domain.JobID, status domain.JobStatus, result []byte, errMsg string) error {
	job, err := u.repo.Get(ctx, id)
	if err != nil {
		return err
	}

	from := job.Status
	switch status {
	case domain.StatusSucceeded:
		err = job.MarkSucceeded(time.Now(), result)
	case domain.StatusFailed:
		err = job.MarkFailed(time.Now(), errMsg)
	case domain.StatusCancelled:
		err = job.MarkCancelled(time.Now())
	default:
		return domain.ErrInvalidTransition
	}
	if err != nil {
		return err
	}

	if err = u.repo.Update(ctx, job); err != nil {
		return err
	}

	u.publishStatus(id, from, job.Status)
	return nil
}

func (u *JobUsecase) ReportEvent(event domain.JobEvent) {
	u.publisher.Publish(event)
}

func (u *JobUsecase) publishStatus(id domain.JobID, from, to domain.JobStatus) {
	u.publisher.Publish(domain.JobEvent{
		JobID:      id,
		OccurredAt: time.Now(),
		Payload:    domain.StatusChanged{From: from, To: to},
	})
}
