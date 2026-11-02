# jo-queue

A small, standalone distributed background job queue written in Go, backed by
Redis. It is built milestone by milestone as a learning project, with the goal
of running real background work for a separate app (JoMoney).

> **Status: M0 (skeleton).** Nothing runs jobs yet. This milestone sets up the
> Go module, folder layout, local Redis, and CI.

## What it will do

- Submit a job and get an ID back right away.
- Many workers process jobs concurrently, and no job is claimed by two workers at once.
- Jobs move through `queued` → `running` → `completed` / `failed`.
- Failed jobs retry with exponential backoff.
- Jobs held by a crashed worker are recovered (leases + heartbeats).
- Idempotency keys stop duplicate submissions.
- Delivery is **at-least-once**, not exactly-once: a job handler may run more
  than once and must be safe to repeat.

## Planned architecture

```
  producer / CLI ──HTTP──▶  queue server (Go)  ──▶  Redis
                                 ▲
  workers (Go or any language) ──┘  HTTP: claim / heartbeat / complete / fail
```

Only the queue server talks to Redis. Workers speak plain HTTP, so they can be
written in any language (JoMoney's worker will be Python).

## Layout

| Path             | Purpose                                               | Arrives in |
|------------------|-------------------------------------------------------|------------|
| `cmd/server`     | HTTP queue server (the only Redis client)             | M3         |
| `cmd/worker`     | Go worker: a pool of goroutines running jobs           | M3         |
| `cmd/cli`        | Submit jobs and inspect status from the terminal      | M3         |
| `internal/queue` | Core queue logic (in-memory in M1, Redis in M2)       | M1 / M2    |
| `compose.yaml`   | Local Redis (server and workers join later)           | M0         |
| `docs/milestones`| Build notes for each milestone                        | each       |

## Quick start (Windows PowerShell)

Requires Go 1.24+ and Docker Desktop.

```powershell
git clone https://github.com/shurjo05/jo-queue.git
cd jo-queue

go vet ./...
go build ./...
go test ./...

docker compose up -d --wait
docker compose exec redis redis-cli ping   # PONG
docker compose down
```

`go test -race` needs a C compiler on Windows, so CI runs the race detector on
Linux for every push and pull request. See [docs/milestones/M0.md](docs/milestones/M0.md).

## Roadmap

| Milestone | What                                                   |
|-----------|--------------------------------------------------------|
| M0        | Skeleton: module, layout, Redis in Compose, CI         |
| M1        | In-memory queue + Go worker pool                       |
| M2        | Redis-backed queue: submit, atomic claim, status       |
| M3        | HTTP server, worker binary, CLI, multi-worker Compose  |
| M4        | Retries, exponential backoff, failed state             |
| M5        | Leases, heartbeats, reaper, crash tests                |
| M6        | Idempotency keys                                       |
| M7        | CSV demo job + benchmarks                              |
| M8        | JoMoney integration (in the JoMoney repo)              |

## License

MIT, see [LICENSE](LICENSE).
