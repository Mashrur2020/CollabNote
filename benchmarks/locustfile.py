"""
Load test for ColabNote API (via Nginx on :80, round-robin to api1/api2).

Each simulated user:
  1. Signs up + logs in once (on_start)
  2. Repeatedly creates notes, lists notes, fetches a note by id, and searches

Run:
  locust -f locustfile.py --host http://localhost --headless \
      -u 50 -r 10 -t 60s --csv=results
"""
import random
import string
import uuid

from locust import HttpUser, task, between


def _rand_word(n=6):
    return "".join(random.choices(string.ascii_lowercase, k=n))


class ColabNoteUser(HttpUser):
    wait_time = between(0.2, 1.0)

    def on_start(self):
        suffix = uuid.uuid4().hex[:10]
        self.username = f"loadtest_{suffix}"
        self.email = f"{self.username}@example.com"
        self.password = "LoadTest123!"

        resp = self.client.post(
            "/api/auth/signup",
            json={
                "username": self.username,
                "email": self.email,
                "password": self.password,
            },
            name="/api/auth/signup",
        )
        if resp.status_code != 201:
            return

        resp = self.client.post(
            "/api/auth/login",
            data={"username": self.username, "password": self.password},
            name="/api/auth/login",
        )
        token = resp.json().get("access_token") if resp.status_code == 200 else None
        self.headers = {"Authorization": f"Bearer {token}"} if token else {}
        self.note_ids = []

    @task(3)
    def create_note(self):
        payload = {
            "title": f"Note {_rand_word()}",
            "content": " ".join(_rand_word(8) for _ in range(20)),
            "tags": [_rand_word(4), _rand_word(4)],
        }
        resp = self.client.post(
            "/api/notes/", json=payload, headers=self.headers, name="/api/notes [POST]"
        )
        if resp.status_code == 201:
            note_id = resp.json().get("id")
            if note_id:
                self.note_ids.append(note_id)

    @task(4)
    def list_notes(self):
        self.client.get("/api/notes/", headers=self.headers, name="/api/notes [GET list]")

    @task(2)
    def get_note(self):
        if not self.note_ids:
            return
        note_id = random.choice(self.note_ids)
        self.client.get(
            f"/api/notes/{note_id}", headers=self.headers, name="/api/notes/:id [GET]"
        )

    @task(2)
    def search_notes(self):
        self.client.get(
            "/api/notes/search/",
            params={"q": _rand_word(4)},
            headers=self.headers,
            name="/api/notes/search [GET]",
        )
