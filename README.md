# ColabNote

A FastAPI backend for a collaborative note-taking application. Built on a polyglot persistence model: PostgreSQL for user accounts, MongoDB for notes and activity logs, Elasticsearch for full-text search, Redis for caching, and Kafka for asynchronous event streaming.

> The core auth, notes CRUD, and activity feed plumbing works end-to-end. The search, cache, and event-publishing layers are built and unit-tested in isolation but only partially wired into the HTTP routes — see [Status](#status) below.

## Table of contents

- [Features](#features)
- [Tech stack](#tech-stack)
- [Architecture](#architecture)
- [Project structure](#project-structure)
- [Quick start](#quick-start)
- [Environment variables](#environment-variables)
- [Running the app](#running-the-app)
- [API endpoints](#api-endpoints)
- [Tests](#tests)
- [Status](#status)
- [Further reading](#further-reading)

## Features

- JWT-based authentication (HS256, bcrypt password hashing)
- Notes CRUD scoped per user (MongoDB)
- Activity feed (last 20 events per user)
- Kafka event publishing (signup / login; note events ready but not yet emitted)
- Standalone Kafka consumer that writes activity logs to MongoDB
- Full-text search helpers backed by Elasticsearch (route is a stub)
- Redis cache helpers (not yet invoked from routes)
- Alembic migrations for the relational schema

## Tech stack

| Layer        | Technology                                           |
| ------------ | ---------------------------------------------------- |
| Framework    | FastAPI 0.115 · Uvicorn 0.32 · Pydantic 2.9          |
| Auth         | bcrypt (passlib) · python-jose JWT (HS256)           |
| RDBMS        | PostgreSQL 16 · SQLAlchemy 2.0 · Alembic 1.13        |
| Document DB  | MongoDB 7 · PyMongo (sync) · Motor (async)           |
| Search       | Elasticsearch 8.11 (AsyncElasticsearch)             |
| Cache        | Redis (`redis.asyncio`)                              |
| Messaging    | Kafka (aiokafka producer + consumer)                 |
| Worker       | Python 3.13 slim container (`consumer/`)             |
| Testing      | pytest 7.4 · pytest-asyncio 0.23 · httpx 0.26        |

## Architecture

```mermaid
flowchart LR
    Client["Client"] -->|JWT| FastAPI["FastAPI<br/>(auth · notes · activity)"]
    FastAPI -->|SQLAlchemy| PG[("PostgreSQL<br/>users")]
    FastAPI -->|PyMongo| Mongo[("MongoDB<br/>notes · activity_logs")]
    FastAPI -.->|planned| ES[("Elasticsearch")]
    FastAPI -.->|planned| Redis[("Redis")]
    FastAPI -->|aiokafka| Kafka[["Kafka<br/>colabnote_events"]]
    Kafka -->|aiokafka| Consumer["consumer/<br/>activity worker"]
    Consumer -->|Motor| Mongo
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full diagram and request lifecycle.

## Project structure

```
ColabNote/
├── app/
│   ├── main.py                 # FastAPI entry, global deps, /ping /profile /users
│   ├── database.py             # SQLAlchemy engine + SessionLocal
│   ├── models.py               # User ORM
│   ├── schemas.py              # Pydantic DTOs (User, Note, Token)
│   ├── auth.py                 # bcrypt + JWT helpers
│   ├── mongodb.py              # PyMongo + Motor clients
│   ├── elasticsearch_client.py # Async ES wrapper
│   ├── redis_client.py         # Async Redis cache
│   ├── events.py               # Kafka producer + log_* helpers
│   ├── kafka_client.py         # (legacy, not imported)
│   └── routers/
│       ├── auth.py             # /api/auth (signup, login)
│       ├── notes.py            # /api/notes (CRUD + search stub)
│       └── activity.py         # /api/activity (read feed)
├── alembic/                    # Users table migration
├── consumer/
│   ├── consumer.py             # aiokafka → Mongo activity_logs
│   ├── Dockerfile              # python:3.13-slim
│   └── requirements.txt
├── tests/
│   ├── test_simple.py          # HTTP smoke (SQLite)
│   ├── test_elasticsearch.py   # ES integration
│   └── test_kafka.py           # full app + mocked broker/cache
├── docker-compose.yml          # postgres, mongo, elasticsearch
├── alembic.ini
├── requirements.txt
├── ARCHITECTURE.md
├── API.md
└── README.md
```

## Quick start

### Prerequisites

- Python 3.13
- Docker + Docker Compose
- (Optional) a running Kafka broker and Redis instance — see [Status](#status)

### 1. Clone and enter the project

```bash
git clone <your-repo-url> ColabNote
cd ColabNote
```

### 2. Create a virtualenv and install dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Configure environment

Copy or create `.env` in the project root. See [Environment variables](#environment-variables) for the full list.

```bash
# Required
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/colabnote
SECRET_KEY=change-me-to-a-long-random-string
MONGO_URL=mongodb://localhost:27017
ES_URL=http://localhost:9200
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
REDIS_HOST=localhost
```

### 4. Start the backing services

```bash
docker compose up -d
```

This brings up PostgreSQL, MongoDB, and Elasticsearch. Redis and Kafka are not in the compose file yet — start them yourself or run the app without those features.

### 5. Apply migrations

```bash
alembic upgrade head
```

### 6. Run the API

```bash
uvicorn app.main:app --reload
```

Visit `http://127.0.0.1:8000/docs` for the interactive OpenAPI UI.

### 7. (Optional) Run the consumer worker

```bash
cd consumer
docker build -t colabnote-consumer .
docker run --rm --env-file ../.env colabnote-consumer
```

Or run it directly if you have Kafka and MongoDB reachable:

```bash
pip install -r consumer/requirements.txt
python -m consumer.consumer
```

## Environment variables

| Variable                       | Default                  | Purpose                                       |
| ------------------------------ | ------------------------ | --------------------------------------------- |
| `APP_NAME`                     | —                        | App display name                              |
| `DATABASE_URL`                 | —                        | SQLAlchemy URL (PostgreSQL or `sqlite:///./test.db` for tests) |
| `MONGO_URL`                    | —                        | MongoDB connection string                     |
| `MONGO_DB`                     | `colabnote`              | Mongo database name                           |
| `SECRET_KEY`                   | —                        | JWT signing key (HS256) — **must be set**     |
| `ALGORITHM`                    | `HS256`                  | JWT algorithm                                 |
| `ACCESS_TOKEN_EXPIRE_MINUTES`  | `30`                     | JWT lifetime                                  |
| `ES_URL`                       | `http://localhost:9200`  | Elasticsearch endpoint                        |
| `ELASTICSEARCH_INDEX`          | `notes`                  | ES index name                                 |
| `REDIS_HOST`                   | `localhost`              | Redis host                                    |
| `REDIS_PORT`                   | `6379`                   | Redis port                                    |
| `CACHE_TTL`                    | `3600`                   | Default cache TTL in seconds                  |
| `KAFKA_BOOTSTRAP_SERVERS`      | `localhost:9092`         | Kafka bootstrap servers                       |
| `ENVIRONMENT`                  | `development`            | Runtime environment                           |
| `LOG_LEVEL`                    | `INFO`                   | Log level                                     |

## Running the app

| Command                                         | What it does                                |
| ----------------------------------------------- | ------------------------------------------- |
| `uvicorn app.main:app --reload`                 | API on `http://127.0.0.1:8000`              |
| `alembic upgrade head`                          | Apply DB migrations                         |
| `alembic revision --autogenerate -m "msg"`      | Create a new migration                      |
| `docker compose up -d`                          | Start PostgreSQL, MongoDB, Elasticsearch    |
| `docker compose down -v`                        | Stop services **and delete volumes**        |

## API endpoints

See [API.md](API.md) for the full reference and diagrams. Summary:

| Method | Path                            | Auth | Purpose                       |
| ------ | ------------------------------- | ---- | ----------------------------- |
| GET    | `/ping`                         | no   | Health check                  |
| POST   | `/api/auth/signup`              | no   | Register a user               |
| POST   | `/api/auth/login`               | no   | Issue JWT (OAuth2 form)       |
| GET    | `/profile`                      | yes  | Current user                  |
| GET    | `/users`                        | yes  | List all users                |
| POST   | `/api/notes/`                   | yes  | Create a note                 |
| GET    | `/api/notes/`                   | yes  | List user's notes             |
| GET    | `/api/notes/search/?q=...`      | yes  | Search (stub: returns `[]`)   |
| GET    | `/api/notes/{note_id}`          | yes  | Get a note                    |
| PUT    | `/api/notes/{note_id}`          | yes  | Update a note                 |
| DELETE | `/api/notes/{note_id}`          | yes  | Delete a note                 |
| GET    | `/api/activity/`                | yes  | Last 20 activity events       |

## Tests

```bash
# HTTP smoke tests (uses local SQLite at ./test.db)
pytest test_simple.py -v

# Elasticsearch integration
pytest test_elasticsearch.py -v

# Full app with mocked Kafka / Mongo / Redis
pytest test_kafka.py -v

# Everything
pytest -v
```

## Status

Implemented and exercised by tests:

- Auth (signup, login, JWT)
- Notes CRUD against MongoDB, scoped per user
- Activity feed read against MongoDB
- Kafka producer + consumer pipeline (signup/login events land in `activity_logs`)
- Elasticsearch and Redis client wrappers

Built but not yet wired into the routes:

- `GET /api/notes/search/?q=…` returns `[]` instead of hitting Elasticsearch
- `POST/PUT/DELETE /api/notes/…` don't publish `log_note_*` events to Kafka
- The Redis cache helpers are not invoked from any router
- `mongodb.setup_indexes()` and `elasticsearch_client.create_index()` are not called at startup

Known sharp edges:

- `app/is_active` is currently `String`, not `Boolean` (carried in both `models.py` and the Alembic migration)
- `KAFKA_BOOTSTRAP_SERVERS=9092` in `.env` is just a port — the code falls back to `localhost:9092`
- `app/kafka_client.py` is dead code; use `app/events.py`
- `get_db` is defined three times (`database.py`, `main.py`, `routers/activity.py`) — the in-module copies shadow the canonical one
- Login form takes `username`, but the JWT carries `sub=email`

## Further reading

- [ARCHITECTURE.md](ARCHITECTURE.md) — full architecture diagrams, request lifecycle, Docker topology
- [API.md](API.md) — endpoint map, request/response shapes, auth flow sequence
