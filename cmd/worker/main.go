// Command worker runs a pool of goroutines that process jobs.
//
// M1: there is no server yet, so this runs a self-contained demo. It creates
// an in-memory queue, submits a batch of demo jobs, lets the pool process
// them, and prints a summary. From M3 the worker claims jobs from the queue
// server over HTTP instead.
//
//	go run ./cmd/worker -workers 4 -jobs 20 -sleep 200ms -fail-every 5
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/shurjo05/jo-queue/internal/jobs"
	"github.com/shurjo05/jo-queue/internal/queue"
	"github.com/shurjo05/jo-queue/internal/worker"
)

func main() {
	workers := flag.Int("workers", 4, "number of worker goroutines")
	numJobs := flag.Int("jobs", 20, "number of demo jobs to submit")
	sleep := flag.Duration("sleep", 200*time.Millisecond, "how long each sleep job takes")
	failEvery := flag.Int("fail-every", 5, "make every Nth job an intentional failure (0 = never)")
	flag.Parse()

	if err := run(*workers, *numJobs, *sleep, *failEvery); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(workers, numJobs int, sleep time.Duration, failEvery int) error {
	// Ctrl+C cancels ctx, which starts a graceful shutdown of the pool.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	q := queue.NewMemoryQueue(numJobs)

	// Submit every job before starting the pool.
	sleepPayload, _ := json.Marshal(jobs.SleepPayload{Millis: int(sleep.Milliseconds())})
	ids := make([]string, 0, numJobs)
	for i := 1; i <= numJobs; i++ {
		jobType, payload := jobs.TypeSleep, sleepPayload
		if failEvery > 0 && i%failEvery == 0 {
			jobType, payload = jobs.TypeFail, nil
		}
		id, err := q.Submit(ctx, jobType, payload)
		if err != nil {
			return fmt.Errorf("submit job %d: %w", i, err)
		}
		ids = append(ids, id)
	}
	fmt.Printf("submitted %d jobs; running with %d workers\n", numJobs, workers)

	// Start the pool in its own goroutine so main can watch progress.
	poolCtx, stopPool := context.WithCancel(ctx)
	poolDone := make(chan struct{})
	pool := &worker.Pool{
		Queue:       q,
		Handlers:    jobs.Handlers(),
		Concurrency: workers,
		Logger:      slog.New(slog.NewTextHandler(os.Stdout, nil)),
	}
	start := time.Now()
	go func() {
		defer close(poolDone)
		_ = pool.Run(poolCtx)
	}()

	// Poll until every job is completed or failed, or Ctrl+C.
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for !allFinished(ctx, q, ids) {
		select {
		case <-ticker.C:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
	}
	elapsed := time.Since(start)
	stopPool()
	<-poolDone

	counts := map[queue.State]int{}
	for _, id := range ids {
		job, _ := q.Get(ctx, id)
		counts[job.State]++
	}
	fmt.Printf("\ndone in %v: %d completed, %d failed, %d queued, %d running\n",
		elapsed.Round(time.Millisecond),
		counts[queue.StateCompleted], counts[queue.StateFailed],
		counts[queue.StateQueued], counts[queue.StateRunning])
	return nil
}

func allFinished(ctx context.Context, q queue.Queue, ids []string) bool {
	for _, id := range ids {
		job, err := q.Get(ctx, id)
		if err != nil || (job.State != queue.StateCompleted && job.State != queue.StateFailed) {
			return false
		}
	}
	return true
}
