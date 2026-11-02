package queue

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// DefaultMemoryCapacity is how many queued jobs a MemoryQueue holds before
// Submit returns ErrQueueFull.
const DefaultMemoryCapacity = 1024

// MemoryQueue is a Queue that lives entirely in process memory.
//
// It is safe for many goroutines at once. Everything is lost when the
// process exits, so it is for tests and for learning the queue's behavior
// before Redis is involved (M2).
//
// How it works:
//   - jobs: a map from ID to job record, guarded by mu.
//   - pending: a buffered channel of IDs waiting to be claimed. A Go channel
//     hands each value to exactly one receiver, which is what gives us
//     "no job is claimed twice" without extra locking.
type MemoryQueue struct {
	mu      sync.Mutex
	jobs    map[string]*Job
	pending chan string
	now     func() time.Time // replaceable in tests
}

// NewMemoryQueue returns an empty queue that holds up to capacity queued
// jobs. A capacity of 0 or less means DefaultMemoryCapacity.
func NewMemoryQueue(capacity int) *MemoryQueue {
	if capacity <= 0 {
		capacity = DefaultMemoryCapacity
	}
	return &MemoryQueue{
		jobs:    make(map[string]*Job),
		pending: make(chan string, capacity),
		now:     time.Now,
	}
}

// Compile-time check that MemoryQueue implements Queue.
var _ Queue = (*MemoryQueue)(nil)

func (q *MemoryQueue) Submit(ctx context.Context, jobType string, payload []byte) (string, error) {
	if jobType == "" {
		return "", fmt.Errorf("queue: job type must not be empty")
	}

	now := q.now()
	job := &Job{
		ID:        newID(),
		Type:      jobType,
		Payload:   append([]byte(nil), payload...), // own copy; caller may reuse theirs
		State:     StateQueued,
		CreatedAt: now,
		UpdatedAt: now,
	}

	// Store the record before the ID becomes claimable, so a worker that
	// receives the ID always finds the job in the map.
	q.mu.Lock()
	q.jobs[job.ID] = job
	q.mu.Unlock()

	select {
	case q.pending <- job.ID:
		return job.ID, nil
	default:
		// Channel buffer is full. Undo the insert and push back on the
		// caller instead of blocking or growing without limit.
		q.mu.Lock()
		delete(q.jobs, job.ID)
		q.mu.Unlock()
		return "", ErrQueueFull
	}
}

func (q *MemoryQueue) Claim(ctx context.Context) (Job, error) {
	// When a job is waiting AND ctx is already cancelled, select below would
	// pick one of the two at random. Checking ctx first makes cancellation win.
	if err := ctx.Err(); err != nil {
		return Job{}, err
	}
	select {
	case id := <-q.pending:
		q.mu.Lock()
		defer q.mu.Unlock()
		job := q.jobs[id]
		job.State = StateRunning
		job.Attempts++
		job.UpdatedAt = q.now()
		return job.clone(), nil
	case <-ctx.Done():
		return Job{}, ctx.Err()
	}
}

func (q *MemoryQueue) Complete(ctx context.Context, id string) error {
	return q.finish(id, StateCompleted, "")
}

func (q *MemoryQueue) Fail(ctx context.Context, id string, reason string) error {
	return q.finish(id, StateFailed, reason)
}

// finish moves a running job to a final state. Only running jobs can
// finish, which stops a job from being completed twice, or completed after
// it already failed.
func (q *MemoryQueue) finish(id string, to State, reason string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, ok := q.jobs[id]
	if !ok {
		return ErrNotFound
	}
	if job.State != StateRunning {
		return fmt.Errorf("%w: job %s is %s, want %s", ErrInvalidState, id, job.State, StateRunning)
	}
	job.State = to
	job.LastError = reason
	job.UpdatedAt = q.now()
	return nil
}

func (q *MemoryQueue) Get(ctx context.Context, id string) (Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, ok := q.jobs[id]
	if !ok {
		return Job{}, ErrNotFound
	}
	return job.clone(), nil
}

// clone returns a deep copy, so callers can't change our record through the
// Job they get back (Payload is a slice, which a plain struct copy shares).
func (j *Job) clone() Job {
	c := *j
	c.Payload = append([]byte(nil), j.Payload...)
	return c
}
