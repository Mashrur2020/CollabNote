# ColabNote Benchmarks

Load-test and throughput scripts for the ColabNote backend, plus the results
captured on 2026-07-10 against the full local docker-compose stack (nginx +
2x FastAPI + PostgreSQL + MongoDB + Elasticsearch + Redis + Kafka).

## Files

- `locustfile.py` — HTTP load test via Nginx (`:80`), round-robins across
  `api1`/`api2`. Each simulated user signs up + logs in once, then repeatedly
  creates notes, lists notes, fetches a note by id, and searches.
- `kafka_bench.py` — standalone Kafka producer throughput test, publishing
  the same event shape as `app/events.py`.
- `results_50users_stats.csv` — Locust output at 50 concurrent users, 60s.
- `results_150users_stats.csv` — Locust output at 150 concurrent users, 45s
  (used to find the breaking point).

## Results summary

**50 concurrent users, 60s, 0% failure rate (4,306 requests):**

| Endpoint | Median | p95 |
|---|---|---|
| Search (Elasticsearch) | 26ms | 80ms |
| List notes (MongoDB) | 11ms | 27ms |
| Create note | 9ms | 28ms |
| Get note by id (Redis cache) | 7ms | 15ms |

Aggregate sustained throughput: ~73 req/s.

**150 concurrent users, 45s:** 9.85% error rate. Auth (`/api/auth/login`,
`/api/auth/signup`) latency balloons to 30-38s and starts throwing 500/403s.
Root cause: signup/login are synchronous (`def`, not `async def`) endpoints
doing bcrypt hashing, which is CPU-bound and saturates FastAPI's thread pool
under concurrent load. Note-serving endpoints (list/create/get/search)
otherwise stay well under 30ms p95 even at this concurrency — the bottleneck
is specifically the auth path, not the notes pipeline.

**Kafka producer throughput:** ~5,400-6,000 events/sec sustained (batches of
500 concurrent sends, 20,000 events total, two runs for consistency). Must be
run from a container on the compose network — see `kafka_bench.py` docstring.

## Reproducing

Requires the full stack up (`docker compose up -d --build` from the repo
root) and `locust` installed in the venv (`pip install locust`).

```bash
# HTTP load test
locust -f locustfile.py --host http://localhost --headless \
    -u 50 -r 10 -t 60s --csv=results

# Kafka throughput (see kafka_bench.py docstring for the docker exec steps)
```

## Known side effect

Installing `locust` pulls in `pytest>=8`, which will upgrade the `pytest`
pinned in `requirements.txt` (`7.4.4`) inside whatever venv you install it
in. Run `pip install pytest==7.4.4` afterward if that breaks the existing
test suite.
