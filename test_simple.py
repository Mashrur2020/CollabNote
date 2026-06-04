"""
Simple test file for CollabNote API
"""
import os

# Set SQLite for testing BEFORE importing app
os.environ["DATABASE_URL"] = "sqlite:///./test.db"

import pytest
from fastapi.testclient import TestClient

# Import the app
from app.main import app
from app.database import Base, engine
from app.models import User  # Import models to register tables


@pytest.fixture(autouse=True)
def setup_database():
    """Create tables before tests, drop after"""
    # Import all models to register them with Base.metadata
    import app.models  # noqa: F401

    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture
def client():
    """Create test client"""
    return TestClient(app)



def test_ping(client):
    """Test ping endpoint"""
    response = client.get("/ping")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "message": "pong"}


def test_signup(client):
    """Test user signup"""
    response = client.post("/api/auth/signup", json={
        "email": "test@example.com",
        "username": "testuser",
        "password": "testpassword123"
    })
    assert response.status_code == 201
    data = response.json()
    assert data["email"] == "test@example.com"
    assert data["username"] == "testuser"
    assert "password" not in data


def test_login(client):
    """Test user login"""
    # First signup
    client.post("/api/auth/signup", json={
        "email": "login@example.com",
        "username": "loginuser",  # Note: login uses username, not email
        "password": "testpassword123"
    })
    
    # Then login - use USERNAME field, not email
    response = client.post("/api/auth/login", data={
        "username": "loginuser",  # Must match the username from signup
        "password": "testpassword123"
    })
    assert response.status_code == 200
    assert "access_token" in response.json()


def test_profile_with_token(client):
    """Test profile endpoint with valid token"""
    # Signup and login
    client.post("/api/auth/signup", json={
        "email": "profile@example.com",
        "username": "profileuser",
        "password": "testpassword123"
    })
    
    login_response = client.post("/api/auth/login", data={
        "username": "profileuser",  # Must match the username from signup
        "password": "testpassword123"
    })
    
    token = login_response.json()["access_token"]
    
    # Get profile
    response = client.get("/profile", headers={
        "Authorization": f"Bearer {token}"
    })
    assert response.status_code == 200
    assert response.json()["email"] == "profile@example.com"


def test_profile_without_token(client):
    """Test profile endpoint without token"""
    response = client.get("/profile")
    assert response.status_code == 403


def test_get_users(client):
    """Test get users endpoint"""
    # Signup and login
    client.post("/api/auth/signup", json={
        "email": "users@example.com",
        "username": "usersuser",
        "password": "testpassword123"
    })
    
    login_response = client.post("/api/auth/login", data={
        "username": "usersuser",  # Must match the username from signup
        "password": "testpassword123"
    })
    
    token = login_response.json()["access_token"]
    
    # Get users
    response = client.get("/users", headers={
        "Authorization": f"Bearer {token}"
    })
    assert response.status_code == 200
    assert isinstance(response.json(), list)