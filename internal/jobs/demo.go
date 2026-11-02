// Package jobs holds demo job handlers used by the Go worker, tests, and
// (from M7) benchmarks. Real application jobs, such as JoMoney's, live in
// the application, not here.
package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/shurjo05/jo-queue/internal/queue"
	"github.com/shurjo05/jo-queue/internal/worker"
)

// Job type names.
const (
	TypeSleep = "sleep"
	TypeFail  = "fail"
)

// SleepPayload is the JSON payload for a "sleep" job.
type SleepPayload struct {
	Millis int `json:"millis"`
}

// Sleep waits for the payload's duration, standing in for real work.
func Sleep(ctx context.Context, job queue.Job) error {
	var p SleepPayload
	if err := json.Unmarshal(job.Payload, &p); err != nil {
		return fmt.Errorf("bad sleep payload: %w", err)
	}
	select {
	case <-time.After(time.Duration(p.Millis) * time.Millisecond):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Fail always returns an error, for testing failure handling.
func Fail(ctx context.Context, job queue.Job) error {
	return errors.New("intentional failure")
}

// Handlers returns every demo handler keyed by job type, ready for
// worker.Pool.Handlers.
func Handlers() map[string]worker.Handler {
	return map[string]worker.Handler{
		TypeSleep: Sleep,
		TypeFail:  Fail,
	}
}
