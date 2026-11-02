package queue

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestSubmitThenGet(t *testing.T) {
	ctx := context.Background()
	q := NewMemoryQueue(0)

	id, err := q.Submit(ctx, "sleep", []byte(`{"millis":1}`))
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	job, err := q.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if job.State != StateQueued || job.Type != "sleep" || string(job.Payload) != `{"millis":1}` {
		t.Fatalf("unexpected job: %+v", job)
	}
}

func TestSubmitRejectsEmptyType(t *testing.T) {
	if _, err := NewMemoryQueue(0).Submit(context.Background(), "", nil); err == nil {
		t.Fatal("expected an error for empty job type")
	}
}

func TestGetUnknownID(t *testing.T) {
	_, err := NewMemoryQueue(0).Get(context.Background(), "nope")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestClaimIsFIFO(t *testing.T) {
	ctx := context.Background()
	q := NewMemoryQueue(0)
	first, _ := q.Submit(ctx, "a", nil)
	second, _ := q.Submit(ctx, "b", nil)

	for _, want := range []string{first, second} {
		job, err := q.Claim(ctx)
		if err != nil {
			t.Fatalf("Claim: %v", err)
		}
		if job.ID != want {
			t.Fatalf("claimed %s, want %s", job.ID, want)
		}
		if job.State != StateRunning || job.Attempts != 1 {
			t.Fatalf("claimed job not marked running: %+v", job)
		}
	}
}

func TestCompleteAndFail(t *testing.T) {
	ctx := context.Background()
	q := NewMemoryQueue(0)
	okID, _ := q.Submit(ctx, "a", nil)
	badID, _ := q.Submit(ctx, "b", nil)
	q.Claim(ctx)
	q.Claim(ctx)

	if err := q.Complete(ctx, okID); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if err := q.Fail(ctx, badID, "boom"); err != nil {
		t.Fatalf("Fail: %v", err)
	}

	ok, _ := q.Get(ctx, okID)
	bad, _ := q.Get(ctx, badID)
	if ok.State != StateCompleted {
		t.Errorf("ok job state = %s, want completed", ok.State)
	}
	if bad.State != StateFailed || bad.LastError != "boom" {
		t.Errorf("bad job = %+v, want failed with LastError boom", bad)
	}
}

func TestOnlyRunningJobsCanFinish(t *testing.T) {
	ctx := context.Background()
	q := NewMemoryQueue(0)
	id, _ := q.Submit(ctx, "a", nil)

	// Still queued: cannot complete.
	if err := q.Complete(ctx, id); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("Complete on queued job: err = %v, want ErrInvalidState", err)
	}

	q.Claim(ctx)
	if err := q.Complete(ctx, id); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	// Already completed: cannot complete again or fail.
	if err := q.Complete(ctx, id); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("second Complete: err = %v, want ErrInvalidState", err)
	}
	if err := q.Fail(ctx, id, "late"); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("Fail after Complete: err = %v, want ErrInvalidState", err)
	}
	if err := q.Complete(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Complete unknown: err = %v, want ErrNotFound", err)
	}
}

func TestSubmitWhenFull(t *testing.T) {
	ctx := context.Background()
	q := NewMemoryQueue(1)
	if _, err := q.Submit(ctx, "a", nil); err != nil {
		t.Fatalf("first Submit: %v", err)
	}
	if _, err := q.Submit(ctx, "a", nil); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("second Submit: err = %v, want ErrQueueFull", err)
	}
	// The rejected job must not linger in the map.
	if got := len(q.jobs); got != 1 {
		t.Fatalf("jobs in map = %d, want 1", got)
	}
}

func TestReturnedJobIsACopy(t *testing.T) {
	ctx := context.Background()
	q := NewMemoryQueue(0)
	payload := []byte("abc")
	id, _ := q.Submit(ctx, "a", payload)
	payload[0] = 'X' // caller reuses its buffer

	job, _ := q.Get(ctx, id)
	job.Payload[1] = 'Y' // caller edits what it got back
	job.State = StateCompleted

	again, _ := q.Get(ctx, id)
	if string(again.Payload) != "abc" || again.State != StateQueued {
		t.Fatalf("stored job was changed from outside: %+v", again)
	}
}

func TestClaimBlocksUntilSubmit(t *testing.T) {
	ctx := context.Background()
	q := NewMemoryQueue(0)

	claimed := make(chan Job)
	go func() {
		job, err := q.Claim(ctx)
		if err != nil {
			t.Errorf("Claim: %v", err)
		}
		claimed <- job
	}()

	select {
	case <-claimed:
		t.Fatal("Claim returned before any job was submitted")
	case <-time.After(50 * time.Millisecond):
	}

	id, _ := q.Submit(ctx, "a", nil)
	select {
	case job := <-claimed:
		if job.ID != id {
			t.Fatalf("claimed %s, want %s", job.ID, id)
		}
	case <-time.After(time.Second):
		t.Fatal("Claim did not return after Submit")
	}
}

func TestClaimStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := NewMemoryQueue(0).Claim(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
}

// TestNoDoubleClaim is the most important property of a queue: with many
// workers claiming at once, every job goes to exactly one of them. Run it
// with -race.
func TestNoDoubleClaim(t *testing.T) {
	const numJobs = 2000
	const claimers = 16

	ctx := context.Background()
	q := NewMemoryQueue(numJobs)
	for range numJobs {
		if _, err := q.Submit(ctx, "a", nil); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}

	var (
		mu     sync.Mutex
		seen   = make(map[string]int)
		wg     sync.WaitGroup
		claims = make(chan struct{}, numJobs) // one token per job to claim
	)
	for range numJobs {
		claims <- struct{}{}
	}
	close(claims)

	for range claimers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range claims {
				job, err := q.Claim(ctx)
				if err != nil {
					t.Errorf("Claim: %v", err)
					return
				}
				mu.Lock()
				seen[job.ID]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(seen) != numJobs {
		t.Fatalf("claimed %d distinct jobs, want %d", len(seen), numJobs)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("job %s claimed %d times", id, n)
		}
	}
}

func TestClaimWithCancelledContextLeavesJobQueued(t *testing.T) {
	q := NewMemoryQueue(0)
	id, _ := q.Submit(context.Background(), "a", nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for range 100 { // select picks randomly, so one try would prove little
		if _, err := q.Claim(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	}
	if job, _ := q.Get(context.Background(), id); job.State != StateQueued {
		t.Fatalf("state = %s, want queued", job.State)
	}
}
