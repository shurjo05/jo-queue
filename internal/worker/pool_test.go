package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shurjo05/jo-queue/internal/queue"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// startPool runs p in the background and returns a function that stops it
// and waits for Run to return.
func startPool(t *testing.T, p *Pool) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	return func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("pool did not stop")
		}
	}
}

// waitForState polls until job id reaches want or the test times out.
func waitForState(t *testing.T, q queue.Queue, id string, want queue.State) queue.Job {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		job, err := q.Get(context.Background(), id)
		if err == nil && job.State == want {
			return job
		}
		time.Sleep(5 * time.Millisecond)
	}
	job, _ := q.Get(context.Background(), id)
	t.Fatalf("job %s: state %s, want %s", id, job.State, want)
	return job
}

func TestPoolCompletesAndFailsJobs(t *testing.T) {
	ctx := context.Background()
	q := queue.NewMemoryQueue(0)
	pool := &Pool{
		Queue: q,
		Handlers: map[string]Handler{
			"ok":    func(context.Context, queue.Job) error { return nil },
			"bad":   func(context.Context, queue.Job) error { return errors.New("boom") },
			"panic": func(context.Context, queue.Job) error { panic("kaboom") },
		},
		Concurrency: 3,
		Logger:      quiet,
	}
	stop := startPool(t, pool)
	defer stop()

	okID, _ := q.Submit(ctx, "ok", nil)
	badID, _ := q.Submit(ctx, "bad", nil)
	panicID, _ := q.Submit(ctx, "panic", nil)
	unknownID, _ := q.Submit(ctx, "no-such-type", nil)

	waitForState(t, q, okID, queue.StateCompleted)

	if job := waitForState(t, q, badID, queue.StateFailed); job.LastError != "boom" {
		t.Errorf("bad job LastError = %q, want boom", job.LastError)
	}
	if job := waitForState(t, q, panicID, queue.StateFailed); !strings.Contains(job.LastError, "kaboom") {
		t.Errorf("panic job LastError = %q, want it to mention kaboom", job.LastError)
	}
	if job := waitForState(t, q, unknownID, queue.StateFailed); !strings.Contains(job.LastError, "no handler") {
		t.Errorf("unknown job LastError = %q, want it to mention no handler", job.LastError)
	}
}

// TestPoolRunsJobsInParallel checks that Concurrency goroutines really run
// at the same time: each handler waits until all of them have started, which
// can only happen if they overlap.
func TestPoolRunsJobsInParallel(t *testing.T) {
	const n = 4
	ctx := context.Background()
	q := queue.NewMemoryQueue(0)

	var started atomic.Int32
	allStarted := make(chan struct{})
	pool := &Pool{
		Queue: q,
		Handlers: map[string]Handler{
			"wait": func(context.Context, queue.Job) error {
				if started.Add(1) == n {
					close(allStarted)
				}
				select {
				case <-allStarted:
					return nil
				case <-time.After(5 * time.Second):
					return errors.New("other workers never started")
				}
			},
		},
		Concurrency: n,
		Logger:      quiet,
	}
	stop := startPool(t, pool)
	defer stop()

	ids := make([]string, n)
	for i := range ids {
		ids[i], _ = q.Submit(ctx, "wait", nil)
	}
	for _, id := range ids {
		waitForState(t, q, id, queue.StateCompleted)
	}
}

// TestPoolShutdownFinishesRunningJob checks graceful shutdown: a job that is
// already running when shutdown starts still completes, and jobs that were
// never claimed stay queued.
func TestPoolShutdownFinishesRunningJob(t *testing.T) {
	ctx := context.Background()
	q := queue.NewMemoryQueue(0)

	running := make(chan struct{})
	release := make(chan struct{})
	pool := &Pool{
		Queue: q,
		Handlers: map[string]Handler{
			"slow": func(ctx context.Context, _ queue.Job) error {
				close(running)
				<-release
				return ctx.Err() // nil, because shutdown must not cancel the job
			},
		},
		Concurrency: 1,
		Logger:      quiet,
	}
	stop := startPool(t, pool)

	slowID, _ := q.Submit(ctx, "slow", nil)
	<-running
	waitingID, _ := q.Submit(ctx, "slow", nil)

	stopped := make(chan struct{})
	go func() { stop(); close(stopped) }()

	select {
	case <-stopped:
		t.Fatal("pool stopped before the running job finished")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	<-stopped

	waitForState(t, q, slowID, queue.StateCompleted)
	if job, _ := q.Get(ctx, waitingID); job.State != queue.StateQueued {
		t.Fatalf("unclaimed job state = %s, want queued", job.State)
	}
}

func TestRunRequiresQueue(t *testing.T) {
	if err := (&Pool{}).Run(context.Background()); err == nil {
		t.Fatal("expected an error when Queue is nil")
	}
}
