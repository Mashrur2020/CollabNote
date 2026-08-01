# ColabNote — API Endpoints

All routes are served by `app/main.py` and mounted under `/api` (except the top-level `/ping`, `/profile`, `/users`).

## Endpoint map

```mermaid
flowchart TD
    Client["Client"]

    subgraph Public["Public"]
        P1["GET /ping"]
        A1["POST /api/auth/signup"]
        A2["POST /api/auth/login<br/>OAuth2 form"]
    end

    subgraph Authed["Authenticated (Bearer JWT)"]
        G1["GET /profile"]
        G2["GET /users"]
        N1["POST /api/notes/"]
        N2["GET /api/notes/"]
        N3["GET /api/notes/search/?q="]
        N4["GET /api/notes/{note_id}"]
        N5["PUT /api/notes/{note_id}"]
        N6["DELETE /api/notes/{note_id}"]
        AC1["GET /api/activity/"]
    end

    Client --> Public
    Client --> Authed

    A1 -. "user_signup" .-> Kafka((Kafka))
    A2 -. "user_login" .-> Kafka((Kafka))

    A1 --> PG[("PostgreSQL<br/>users")]
    A2 --> PG
    G1 --> PG
    G2 --> PG

    N1 --> Mongo[("MongoDB<br/>notes")]
    N2 --> Mongo
    N3 -. "ES (planned)" .-> ES[("Elasticsearch")]
    N4 --> Mongo
    N5 --> Mongo
    N6 --> Mongo

    AC1 --> Mongo
    AC1 --> PG

    classDef planned stroke-dasharray:5 5,fill:#fff4e1,stroke:#ef6c00,color:#000
    class N3,ES planned
```

## Endpoint reference

Legend: **Auth** = bearer JWT required · **Kafka** = publishes an event · **Store** = backend hit on the request path

### Top-level (`app/main.py`)

| Method | Path       | Auth | Request                | Response                  | Store |
| ------ | ---------- | ---- | ---------------------- | ------------------------- | ----- |
| GET    | `/ping`    | no   | —                      | `{status, message}`       | —     |
| GET    | `/profile` | yes  | —                      | `UserOut`                 | PG    |
| GET    | `/users`   | yes  | —                      | `list[UserOut]`           | PG    |

### Auth (`/api/auth`, `app/routers/auth.py`)

| Method | Path                  | Auth | Request / Form                                                  | Response      | Store | Kafka             |
| ------ | --------------------- | ---- | --------------------------------------------------------------- | ------------- | ----- | ----------------- |
| POST   | `/api/auth/signup`    | no   | JSON `{email, username, password}` (`UserCreate`)               | `UserOut` 201 | PG    | `user_signup`     |
| POST   | `/api/auth/login`     | no   | OAuth2 form: `username`, `password` (`OAuth2PasswordRequestForm`) | `Token`       | PG    | `user_login`      |

> `signup` returns the created user; `login` returns `{access_token, token_type: "bearer"}` (JWT HS256, `sub=email`, 30 min default).

### Notes (`/api/notes`, `app/routers/notes.py`)

| Method | Path                       | Auth | Request                                            | Response        | Store | Kafka |
| ------ | -------------------------- | ---- | -------------------------------------------------- | --------------- | ----- | ----- |
| POST   | `/api/notes/`              | yes  | JSON `{title, content}` (`NoteCreate`)             | `NoteOut` 201   | Mongo | —     |
| GET    | `/api/notes/`              | yes  | —                                                  | `list[NoteOut]` | Mongo | —     |
| GET    | `/api/notes/search/?q=…`   | yes  | query `q`                                          | `[]` (stub)     | ES (planned) | `log_note_searched` (planned) |
| GET    | `/api/notes/{note_id}`     | yes  | path `note_id`                                     | `NoteOut` 404 if missing | Mongo | — |
| PUT    | `/api/notes/{note_id}`     | yes  | JSON partial `{title?, content?}` (`NoteUpdate`)   | `NoteOut`       | Mongo | —     |
| DELETE | `/api/notes/{note_id}`     | yes  | path `note_id`                                     | `{message}` 404 if missing | Mongo | — |

> The `notes` router decodes the JWT itself (`get_current_user_email` in-module) and uses `email` to scope every query. **Create / update / delete publish Kafka events, update Elasticsearch, and manage Redis cache** via `BackgroundTasks`.

### Activity (`/api/activity`, `app/routers/activity.py`)

| Method | Path              | Auth | Request | Response                                                                                  | Store              |
| ------ | ----------------- | ---- | ------- | ----------------------------------------------------------------------------------------- | ------------------ |
| GET    | `/api/activity/`  | yes  | —       | `{activities: [...], count}` — last 20 `activity_logs` for the user, sorted by timestamp desc | Mongo (`activity_logs`) + PG (user lookup) |

Each activity entry: `{event_type, user_id, resource_id, timestamp, metadata}`.

## Auth flow at a glance

```mermaid
sequenceDiagram
    participant C as Client
    participant A as /api/auth
    participant N as /api/notes
    participant AC as /api/activity

    C->>A: POST /signup {email, username, password}
    A-->>C: 201 UserOut
    C->>A: POST /login (form: username, password)
    A-->>C: {access_token}
    Note over C: Authorization: Bearer <token>

    C->>N: POST /api/notes/  (Bearer)
    N-->>C: 201 NoteOut
    C->>N: GET /api/notes/{id}
    N-->>C: 200 NoteOut
    C->>N: PUT /api/notes/{id}
    N-->>C: 200 NoteOut
    C->>N: DELETE /api/notes/{id}
    N-->>C: 200 {message}
    C->>N: GET /api/notes/search/?q=foo
    N-->>C: 200 []  (stub)

    C->>AC: GET /api/activity/  (Bearer)
    AC-->>C: 200 {activities, count}
```
