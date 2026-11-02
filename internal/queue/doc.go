// Package queue will hold the core job queue logic: job states, submitting,
// claiming, completing, retries, and leases.
//
// It lives under internal/ so that only code inside this module can import
// it. The public surface of jo-queue is the HTTP API, not this Go package.
//
// M0: empty on purpose. M1 adds an in-memory queue; M2 adds the Redis one.
package queue
