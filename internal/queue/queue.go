package queue

import (
	"context"
	"errors"
	"time"
)

// State is where a job is in its life cycle.
//
//	queued ──Claim──▶ running ──Complete──▶ completed
//	                     │
//	                     └────Fail───────▶ failed
//
// M1 has no retries, so failed is final. M4 adds retries with backoff.
type State string

const (
	StateQueued    State = "queued"
	StateRunning   State = "running"
	StateCompleted State = "completed"
	StateFailed    State = "failed"
)

// Job is one unit of background work.
//
// Payload is opaque bytes (usually JSON). The queue never looks inside it;
// only the handler for Type knows what it means. Payloads should carry IDs
// and references, not large data.
type Job struct {
	ID        string
	Type      string
	Payload   []byte
	State     State
	Attempts  int    // how many times the job has been claimed
	LastError string // set when the job fails
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Errors returned by Queue implementations. Callers compare with errors.Is.
var (
	ErrNotFound     = errors.New("queue: job not found")
	ErrInvalidState = errors.New("queue: job is not in a state that allows this")
	ErrQueueFull    = errors.New("queue: queue is full")
)

// Queue is the contract every queue implementation follows, in memory (M1)
// or in Redis (M2). In M3 an HTTP client will implement it too, so a worker
// cannot tell whether the queue is local or across the network.
type Queue interface {
	// Submit stores a new job in the queued state and returns its ID at once.
	Submit(ctx context.Context, jobType string, payload []byte) (string, error)

	// Claim hands the oldest queued job to exactly one caller and marks it
	// running. It blocks until a job is available or ctx is done, in which
	// case it returns ctx.Err().
	Claim(ctx context.Context) (Job, error)

	// Complete marks a running job completed.
	Complete(ctx context.Context, id string) error

	// Fail marks a running job failed and records why.
	Fail(ctx context.Context, id string, reason string) error

	// Get returns a copy of the job's current record.
	Get(ctx context.Context, id string) (Job, error)
}
