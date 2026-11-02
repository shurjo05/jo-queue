// Package worker runs jobs: a Pool of goroutines that repeatedly claim a job
// from a queue.Queue, run the handler registered for its type, and report
// the result back to the queue.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/shurjo05/jo-queue/internal/queue"
)

// Handler runs one job. Returning nil marks the job completed; returning an
// error marks it failed with the error's message.
//
// Delivery is at-least-once (from M5 on, a job can run again if a worker
// dies mid-job), so a handler must be safe to run more than once.
type Handler func(ctx context.Context, job queue.Job) error

// Pool runs Concurrency goroutines that each process one job at a time.
type Pool struct {
	Queue       queue.Queue
	Handlers    map[string]Handler // job type -> handler
	Concurrency int                // number of worker goroutines; < 1 means 1
	Logger      *slog.Logger       // nil means slog.Default()
}

// Run starts the workers and blocks until ctx is cancelled.
//
// Shutdown is graceful: once ctx is cancelled, workers stop claiming new
// jobs, but a job that is already running gets to finish and report its
// result. Run returns after every worker has stopped.
func (p *Pool) Run(ctx context.Context) error {
	if p.Queue == nil {
		return errors.New("worker: Pool.Queue is nil")
	}
	n := max(p.Concurrency, 1)
	log := p.Logger
	if log == nil {
		log = slog.Default()
	}

	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.loop(ctx, log.With("worker", i))
		}()
	}
	wg.Wait()
	return nil
}

// loop is one worker goroutine: claim, run, report, repeat.
func (p *Pool) loop(ctx context.Context, log *slog.Logger) {
	for {
		// Check for shutdown first. Without this, a worker that just
		// finished a job could still claim another one after shutdown began.
		if ctx.Err() != nil {
			return
		}
		job, err := p.Queue.Claim(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return // shutting down
			}
			// The in-memory queue only fails Claim on cancellation, but a
			// network-backed queue (M2/M3) can fail for real. Log and retry.
			log.Error("claim failed, retrying in 1s", "err", err)
			select {
			case <-time.After(time.Second):
			case <-ctx.Done():
				return
			}
			continue
		}

		// The job was claimed, so finish it even if shutdown starts now.
		// WithoutCancel keeps ctx's values but ignores its cancellation.
		jobCtx := context.WithoutCancel(ctx)
		log := log.With("job_id", job.ID, "type", job.Type)

		if runErr := p.run(jobCtx, job); runErr != nil {
			log.Warn("job failed", "err", runErr)
			if err := p.Queue.Fail(jobCtx, job.ID, runErr.Error()); err != nil {
				log.Error("reporting failure", "err", err)
			}
			continue
		}
		if err := p.Queue.Complete(jobCtx, job.ID); err != nil {
			log.Error("reporting completion", "err", err)
			continue
		}
		log.Info("job completed")
	}
}

// run calls the job's handler and turns a panic into an ordinary error, so
// one bad job can't crash the whole worker process.
func (p *Pool) run(ctx context.Context, job queue.Job) (err error) {
	h, ok := p.Handlers[job.Type]
	if !ok {
		return fmt.Errorf("no handler registered for job type %q", job.Type)
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panicked: %v\n%s", r, debug.Stack())
		}
	}()
	return h(ctx, job)
}
