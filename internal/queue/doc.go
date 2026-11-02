// Package queue holds the core job queue: the Job type, its states, the
// Queue interface, and implementations of it.
//
// M1 has one implementation, MemoryQueue, which keeps everything in process
// memory. M2 adds a Redis-backed implementation of the same interface, so the
// worker pool and tests written against Queue keep working unchanged.
//
// It lives under internal/ so that only code inside this module can import
// it. The public surface of jo-queue will be the HTTP API (M3).
package queue
