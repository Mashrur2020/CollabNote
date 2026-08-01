"""HTTP smoke tests for ColabNote API using SQLite in-memory database."""
import pytest
import pytest_asyncio
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
    """Create a test user and return it."""
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
async def test_ping(client):
    """GET /ping should return 200 with status ok."""
    response = await client.get("/ping")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["message"] == "pong"


@pytest.mark.asyncio
async def test_signup(client):
    """POST /api/auth/signup should create a new user."""
    response = await client.post(
        "/api/auth/signup",
        json={
            "email": "alice@example.com",
            "username": "alice123",
            "password": "securepass123",
        },
    )
    assert response.status_code == 201
    data = response.json()
    assert data["email"] == "alice@example.com"
    assert data["username"] == "alice123"
    assert "id" in data


@pytest.mark.asyncio
async def test_signup_duplicate_email(client, test_user):
    """POST /api/auth/signup with existing email should return 400."""
    response = await client.post(
        "/api/auth/signup",
        json={
            "email": test_user.email,
            "username": "anotheruser",
            "password": "securepass123",
        },
    )
    assert response.status_code == 400
    assert "already registered" in response.json()["detail"]


@pytest.mark.asyncio
async def test_login(client, test_user):
    """POST /api/auth/login should return a JWT token."""
    response = await client.post(
        "/api/auth/login",
        data={"username": test_user.username, "password": "testpassword123"},
    )
    assert response.status_code == 200
    data = response.json()
    assert "access_token" in data
    assert data["token_type"] == "bearer"


@pytest.mark.asyncio
async def test_login_wrong_password(client, test_user):
    """POST /api/auth/login with wrong password should return 401."""
    response = await client.post(
        "/api/auth/login",
        data={"username": test_user.username, "password": "wrongpassword"},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_profile_authenticated(client, test_user, auth_token):
    """GET /profile with valid token should return user profile."""
    response = await client.get(
        "/profile",
        headers={"Authorization": f"Bearer {auth_token}"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["email"] == test_user.email
    assert data["username"] == test_user.username


@pytest.mark.asyncio
async def test_profile_unauthenticated(client):
    """GET /profile without token should return 401."""
    response = await client.get("/profile")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_get_users(client, test_user, auth_token):
    """GET /users should return list of users."""
    response = await client.get(
        "/users",
        headers={"Authorization": f"Bearer {auth_token}"},
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    assert any(u["email"] == test_user.email for u in data)