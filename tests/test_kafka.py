"""Kafka integration tests for ColabNote event publishing.

These tests use the full app with mocked Kafka broker and cache.
They verify that events are published correctly through the event system.
"""
import pytest
import pytest_asyncio
from unittest.mock import patch, AsyncMock
from httpx import AsyncClient, ASGITransport
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.database import Base, get_db
from app.main import app
from app.auth import hash_password, create_access_token
from app.models import User


@pytest.fixture
def db_session():
    """Create a fresh in-memory SQLite database for each test."""
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    TestSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = TestSessionLocal()
    try:
        yield session
    finally:
        session.close()
        Base.metadata.drop_all(bind=engine)


@pytest_asyncio.fixture
async def client(db_session):
    """Override the get_db dependency with our test session."""

    def _get_db_override():
        yield db_session

    app.dependency_overrides[get_db] = _get_db_override
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
    app.dependency_overrides.clear()


@pytest.fixture
def test_user(db_session):
    """Create a test user."""
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


@pytest.mark.asyncio
@patch("app.events._producer", new_callable=AsyncMock)
@patch("app.events._queue")
async def test_signup_publishes_event(mock_queue, mock_producer, client):
    """Signup should enqueue a user_signup event."""
    mock_queue.put_nowait = AsyncMock()

    response = await client.post(
        "/api/auth/signup",
        json={
            "email": "newuser@example.com",
            "username": "newuser123",
            "password": "securepass123",
        },
    )
    assert response.status_code == 201
    mock_queue.put_nowait.assert_called_once()
    call_args = mock_queue.put_nowait.call_args[0][0]
    assert call_args["event_type"] == "user_signup"
    assert call_args["metadata"]["email"] == "newuser@example.com"


@pytest.mark.asyncio
@patch("app.events._producer", new_callable=AsyncMock)
@patch("app.events._queue")
async def test_login_publishes_event(mock_queue, mock_producer, client, test_user):
    """Login should enqueue a user_login event."""
    mock_queue.put_nowait = AsyncMock()

    response = await client.post(
        "/api/auth/login",
        data={"username": test_user.username, "password": "testpassword123"},
    )
    assert response.status_code == 200
    mock_queue.put_nowait.assert_called_once()
    call_args = mock_queue.put_nowait.call_args[0][0]
    assert call_args["event_type"] == "user_login"
    assert call_args["metadata"]["email"] == test_user.email


@pytest.mark.asyncio
@patch("app.events._producer", new_callable=AsyncMock)
@patch("app.events._queue")
async def test_event_queue_full_drops_event(mock_queue, mock_producer):
    """When the event queue is full, events should be dropped silently."""
    from app.events import publish_event

    mock_queue.full.return_value = True
    mock_queue.put_nowait.side_effect = asyncio.QueueFull

    # Should not raise an exception
    publish_event("test_event", user_id=1)
    assert True


import asyncio