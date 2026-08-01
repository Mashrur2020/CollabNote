"""Shared fixtures for CollabNote tests.

Provides an in-memory SQLite DB (for PostgreSQL-backed endpoints), an
in-memory fake MongoDB, an in-memory fake Redis, and a mocked
Elasticsearch client so note CRUD / caching / activity / GraphQL tests
run without external services.
"""
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from bson import ObjectId
from httpx import ASGITransport, AsyncClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.auth import create_access_token, hash_password
from app.database import Base, get_db
from app.main import app
from app.models import User


class FakeCursor:
    """Minimal async cursor supporting sort/limit/to_list."""

    def __init__(self, docs, sort_key=None, reverse=False, limit_n=None):
        self._docs = docs
        self._sort_key = sort_key
        self._reverse = reverse
        self._limit_n = limit_n

    def sort(self, key, direction):
        return FakeCursor(
            self._docs,
            sort_key=key,
            reverse=(direction == -1),
            limit_n=self._limit_n,
        )

    def limit(self, n):
        return FakeCursor(
            self._docs,
            sort_key=self._sort_key,
            reverse=self._reverse,
            limit_n=n,
        )

    async def to_list(self, length=None):
        docs = self._docs
        if self._sort_key:
            docs = sorted(
                docs,
                key=lambda d: str(d.get(self._sort_key) or ""),
                reverse=self._reverse,
            )
        n = self._limit_n if self._limit_n is not None else length or len(docs)
        return list(docs[:n])


class FakeCollection:
    """In-memory async MongoDB collection."""

    def __init__(self):
        self._docs = []

    def _matches(self, doc, query):
        for key, value in query.items():
            if doc.get(key) != value:
                return False
        return True

    async def insert_one(self, doc):
        doc = dict(doc)
        doc["_id"] = ObjectId()
        self._docs.append(doc)
        return SimpleNamespace(inserted_id=doc["_id"])

    async def find_one(self, query):
        for doc in self._docs:
            if self._matches(doc, query):
                return dict(doc)
        return None

    def find(self, query):
        docs = [dict(d) for d in self._docs if self._matches(d, query)]
        return FakeCursor(docs)

    async def update_one(self, query, update):
        for doc in self._docs:
            if self._matches(doc, query):
                doc.update(update.get("$set", {}))
                return SimpleNamespace(matched_count=1, modified_count=1)
        return SimpleNamespace(matched_count=0, modified_count=0)

    async def delete_one(self, query):
        for i, doc in enumerate(self._docs):
            if self._matches(doc, query):
                self._docs.pop(i)
                return SimpleNamespace(deleted_count=1)
        return SimpleNamespace(deleted_count=0)


class FakeRedis:
    """In-memory fake for the app's RedisClient."""

    def __init__(self):
        self.store = {}

    async def get_note(self, note_id):
        return self.store.get(f"note:{note_id}")

    async def set_note(self, note_id, note_data):
        self.store[f"note:{note_id}"] = note_data

    async def invalidate_note(self, note_id):
        self.store.pop(f"note:{note_id}", None)

    async def invalidate_search_cache(self, user_id):
        for key in list(self.store):
            if key.startswith(f"search:{user_id}:"):
                self.store.pop(key, None)

    async def set_search_results(self, user_id, query, results):
        self.store[f"search:{user_id}:{query}"] = results


@pytest.fixture
def db_session():
    """Create a fresh in-memory SQLite database for each test."""
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    factory = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = factory()
    # GraphQL get_context builds its own SessionLocal; point it at the same DB.
    with patch("app.graphql.context.SessionLocal", factory):
        yield session
    session.close()
    Base.metadata.drop_all(bind=engine)


@pytest.fixture
def fake_mongo():
    """In-memory fake Mongo with notes + activity_logs collections."""
    db = {
        "notes": FakeCollection(),
        "activity_logs": FakeCollection(),
    }
    patches = [
        patch("app.routers.notes.async_db", db),
        patch("app.routers.activity.async_db", db),
        patch("app.main.async_db", db),
        patch("app.graphql.context.async_db", db),
    ]
    for p in patches:
        p.start()
    yield db
    for p in patches:
        p.stop()


@pytest.fixture
def fake_redis():
    """In-memory fake Redis bound to the notes + GraphQL modules."""
    r = FakeRedis()
    patches = [
        patch("app.routers.notes.redis_client", r),
        patch("app.graphql.mutations.redis_client", r),
    ]
    for p in patches:
        p.start()
    yield r
    for p in patches:
        p.stop()


@pytest.fixture
def fake_es():
    """Mocked Elasticsearch client module."""
    mock = MagicMock()
    mock.index_note = AsyncMock()
    mock.update_note_index = AsyncMock()
    mock.delete_note_index = AsyncMock()
    mock.search_notes = AsyncMock(return_value={"total": 0, "results": []})
    patches = [
        patch("app.routers.notes.elasticsearch_client", mock),
        patch("app.graphql.mutations.elasticsearch_client", mock),
    ]
    for p in patches:
        p.start()
    yield mock
    for p in patches:
        p.stop()


@pytest_asyncio.fixture
async def client(db_session, fake_mongo, fake_redis, fake_es):
    """ASGI test client with overridden DB + in-memory Mongo/Redis/ES."""

    def _get_db_override():
        yield db_session

    app.dependency_overrides[get_db] = _get_db_override
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
    app.dependency_overrides.clear()


@pytest.fixture
def test_user(db_session):
    """Create a test user in the in-memory SQLite DB."""
    user = User(
        email="test@example.com",
        username="testuser",
        password_hash=hash_password("testpassword123"),
    )
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)
    return user


@pytest.fixture
def auth_token(test_user):
    """Generate a valid JWT for the test user."""
    return create_access_token(data={"sub": test_user.email})


@pytest.fixture
def auth_headers(auth_token):
    return {"Authorization": f"Bearer {auth_token}"}
