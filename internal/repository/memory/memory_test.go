package memory_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mao360/jobqueue-scheduler/internal/domain"
	"github.com/mao360/jobqueue-scheduler/internal/repository/memory"
)

func newJob(t *testing.T, id string) *domain.Job {
	t.Helper()
	job, err := domain.NewJob(domain.JobID(id), "echo", []byte("payload"), time.Second, time.Now())
	require.NoError(t, err)
	return job
}

func TestMemory_Create(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		prepare func(t *testing.T, m *memory.Memory) // состояние store до проверяемого вызова
		id      string
		wantErr error
	}{
		{
			name:    "ok",
			prepare: func(*testing.T, *memory.Memory) {},
			id:      "job-1",
			wantErr: nil,
		},
		{
			name: "duplicate id",
			prepare: func(t *testing.T, m *memory.Memory) {
				require.NoError(t, m.Create(context.Background(), newJob(t, "job-1")))
			},
			id:      "job-1",
			wantErr: domain.ErrJobAlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := memory.New()
			tt.prepare(t, m)

			err := m.Create(context.Background(), newJob(t, tt.id))

			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestMemory_Get(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		prepare func(t *testing.T, m *memory.Memory)
		id      string
		wantErr error
	}{
		{
			name: "existing",
			prepare: func(t *testing.T, m *memory.Memory) {
				require.NoError(t, m.Create(context.Background(), newJob(t, "job-1")))
			},
			id:      "job-1",
			wantErr: nil,
		},
		{
			name:    "missing",
			prepare: func(*testing.T, *memory.Memory) {},
			id:      "nope",
			wantErr: domain.ErrJobNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := memory.New()
			tt.prepare(t, m)

			got, err := m.Get(context.Background(), domain.JobID(tt.id))

			require.ErrorIs(t, err, tt.wantErr)
			if tt.wantErr != nil {
				require.Nil(t, got)
				return
			}
			require.Equal(t, domain.JobID(tt.id), got.ID)
			require.Equal(t, "echo", got.TaskType)
			require.Equal(t, []byte("payload"), got.Payload)
			require.Equal(t, domain.StatusPending, got.Status)
		})
	}
}

func TestMemory_Update(t *testing.T) {
	t.Parallel()

	t.Run("visible in subsequent Get", func(t *testing.T) {
		t.Parallel()

		m := memory.New()
		require.NoError(t, m.Create(context.Background(), newJob(t, "job-1")))

		job, err := m.Get(context.Background(), "job-1")
		require.NoError(t, err)
		require.NoError(t, job.MarkRunning(time.Now(), "worker-1"))
		require.NoError(t, m.Update(context.Background(), job))

		got, err := m.Get(context.Background(), "job-1")
		require.NoError(t, err)
		require.Equal(t, domain.StatusRunning, got.Status)
		require.Equal(t, domain.WorkerID("worker-1"), got.AssignedWorkerID)
	})

	t.Run("missing returns ErrJobNotFound", func(t *testing.T) {
		t.Parallel()

		m := memory.New()
		err := m.Update(context.Background(), newJob(t, "ghost"))
		require.ErrorIs(t, err, domain.ErrJobNotFound)
	})
}

// Проверяет, что store отдаёт копии: мутация job'а, полученного из Get,
// не должна менять состояние внутри store. Ваша реализация это делает
// (копирование при Create/Get/Update) — тест фиксирует инвариант,
// чтобы будущий рефакторинг его не сломал.
func TestMemory_GetReturnsCopy(t *testing.T) {
	t.Parallel()

	m := memory.New()
	require.NoError(t, m.Create(context.Background(), newJob(t, "job-1")))

	got, err := m.Get(context.Background(), "job-1")
	require.NoError(t, err)

	got.Status = domain.StatusFailed // мутируем копию

	fresh, err := m.Get(context.Background(), "job-1")
	require.NoError(t, err)
	require.Equal(t, domain.StatusPending, fresh.Status)
}

// Смысл теста — не в assert'ах, а в том, что он гоняется под `go test -race`:
// детектор ловит небезопасный конкурентный доступ к map. Без -race тест
// почти бесполезен.
func TestMemory_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	const (
		goroutines   = 16
		jobsPerGoroutine = 50
	)

	m := memory.New()
	ctx := context.Background()

	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobsPerGoroutine {
				id := fmt.Sprintf("job-%d-%d", g, i)

				job, err := domain.NewJob(domain.JobID(id), "echo", nil, time.Second, time.Now())
				require.NoError(t, err)

				require.NoError(t, m.Create(ctx, job))

				got, err := m.Get(ctx, domain.JobID(id))
				require.NoError(t, err)

				require.NoError(t, got.MarkRunning(time.Now(), "worker-1"))
				require.NoError(t, m.Update(ctx, got))
			}
		}()
	}
	wg.Wait()
	
	for g := range goroutines {
		for i := range jobsPerGoroutine {
			id := domain.JobID(fmt.Sprintf("job-%d-%d", g, i))

			got, err := m.Get(ctx, id)
			require.NoError(t, err)
			require.Equal(t, domain.StatusRunning, got.Status)
		}
	}
}