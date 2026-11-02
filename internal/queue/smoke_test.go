package queue

import (
	"sync"
	"testing"
)

// TestRaceDetectorSmoke exists only so that CI has a concurrent test to run
// under `go test -race` before any real queue code exists. It starts many
// goroutines that bump a shared counter behind a mutex. If you remove the
// mutex (the mu variable and its Lock/Unlock calls), `go test -race` fails
// with a DATA RACE report: the safety net we want for the real queue.
//
// Delete this test in M1, once real concurrent tests replace it.
func TestRaceDetectorSmoke(t *testing.T) {
	const goroutines = 50
	const incrementsEach = 1000

	var (
		mu      sync.Mutex
		counter int
		wg      sync.WaitGroup
	)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < incrementsEach; j++ {
				mu.Lock()
				counter++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if want := goroutines * incrementsEach; counter != want {
		t.Fatalf("counter = %d, want %d", counter, want)
	}
}
