package jobs

import (
	"context"
	"testing"
	"time"

	"github.com/shurjo05/jo-queue/internal/queue"
)

func TestSleep(t *testing.T) {
	start := time.Now()
	if err := Sleep(context.Background(), queue.Job{Payload: []byte(`{"millis":20}`)}); err != nil {
		t.Fatalf("Sleep: %v", err)
	}
	if time.Since(start) < 20*time.Millisecond {
		t.Fatal("Sleep returned too early")
	}
}

func TestSleepBadPayload(t *testing.T) {
	if err := Sleep(context.Background(), queue.Job{Payload: []byte(`not json`)}); err == nil {
		t.Fatal("expected an error for a bad payload")
	}
}

func TestFail(t *testing.T) {
	if err := Fail(context.Background(), queue.Job{}); err == nil {
		t.Fatal("expected an error")
	}
}
