package eventbus

import (
	"sync"

	"github.com/mao360/jobqueue-scheduler/internal/domain"
)

const bufLen = 32

type Bus struct {
	mu     sync.Mutex
	subs   map[domain.JobID]map[chan domain.JobEvent]struct{}
	closed bool
}

func New() *Bus {
	return &Bus{
		subs: make(map[domain.JobID]map[chan domain.JobEvent]struct{}),
	}
}

func (b *Bus) Subscribe(jobID domain.JobID) (<-chan domain.JobEvent, func()) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		ch := make(chan domain.JobEvent)
		close(ch)
		return ch, func() {}
	}

	ch := make(chan domain.JobEvent, bufLen)

	if _, ok := b.subs[jobID]; !ok {
		b.subs[jobID] = make(map[chan domain.JobEvent]struct{})
	}
	b.subs[jobID][ch] = struct{}{}

	return ch, func() {
		b.mu.Lock()
		defer b.mu.Unlock()

		if _, ok := b.subs[jobID][ch]; ok {
			delete(b.subs[jobID], ch)
			close(ch)

			if len(b.subs[jobID]) == 0 {
				delete(b.subs, jobID)
			}
		}
	}
}

func (b *Bus) Publish(event domain.JobEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for ch := range b.subs[event.JobID] {
		select {
		case ch <- event:
		default:
		}
	}
}

func (b *Bus) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for jobID := range b.subs {
		for ch := range b.subs[jobID] {
			close(ch)
		}
	}

	b.subs = nil
	b.closed = true
}
