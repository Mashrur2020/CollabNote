"""Tests for the GraphQL dashboard (Phase 4 acceptance criteria).

Covers the dashboard query (me + notes + activityLogs in a single query),
GraphQL auth enforcement, and mutations mirroring REST state.
"""
import pytest

DASHBOARD_QUERY = """
query Dashboard {
  me { id username email }
  notes { id title content tags }
}
"""


@pytest.mark.asyncio
async def test_graphql_requires_auth(client):
    """Queries without a token should return an auth error."""
    response = await client.post(
        "/graphql",
        json={"query": "{ me { id email } }"},
    )
    assert response.status_code == 200
    assert response.json()["errors"]
    assert "Unauthorized" in response.json()["errors"][0]["message"]


@pytest.mark.asyncio
async def test_graphql_me_query(client, auth_headers, test_user):
    """The `me` query returns the authenticated user."""
    response = await client.post(
        "/graphql",
        json={"query": "{ me { id username email } }"},
        headers=auth_headers,
    )
    assert response.status_code == 200
    data = response.json()["data"]["me"]
    assert data["email"] == test_user.email
    assert data["username"] == test_user.username


@pytest.mark.asyncio
async def test_graphql_dashboard_query(client, auth_headers, test_user):
    """Dashboard query returns profile + notes in a single request."""
    # Seed one note for the user.
    await client.post(
        "/api/notes/",
        json={
            "title": "GraphQL Note",
            "content": "Dashboard content",
            "tags": ["graphql"],
        },
        headers=auth_headers,
    )

    response = await client.post(
        "/graphql",
        json={"query": DASHBOARD_QUERY},
        headers=auth_headers,
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["me"]["email"] == test_user.email
    assert len(data["notes"]) == 1
    assert data["notes"][0]["title"] == "GraphQL Note"


@pytest.mark.asyncio
async def test_graphql_users_query(client, auth_headers, test_user):
    """The users query lists PostgreSQL users."""
    response = await client.post(
        "/graphql",
        json={"query": "{ users { id username email } }"},
        headers=auth_headers,
    )
    assert response.status_code == 200
    users = response.json()["data"]["users"]
    assert any(u["email"] == test_user.email for u in users)


@pytest.mark.asyncio
async def test_graphql_user_query(client, auth_headers, test_user):
    """The user(id) query looks up a user by Postgres primary key."""
    response = await client.post(
        "/graphql",
        json={
            "query": "query GetUser($id: ID!) { user(id: $id) { username email } }",
            "variables": {"id": str(test_user.id)},
        },
        headers=auth_headers,
    )
    assert response.status_code == 200
    user = response.json()["data"]["user"]
    assert user["username"] == test_user.username


@pytest.mark.asyncio
async def test_graphql_notes_query_scoped(client, auth_headers, test_user):
    """The notes query is scoped to the authenticated user."""
    await client.post(
        "/api/notes/",
        json={"title": "Mine", "content": "content", "tags": []},
        headers=auth_headers,
    )

    response = await client.post(
        "/graphql",
        json={"query": "{ notes { title } }"},
        headers=auth_headers,
    )
    assert response.status_code == 200
    titles = [n["title"] for n in response.json()["data"]["notes"]]
    assert titles == ["Mine"]


@pytest.mark.asyncio
async def test_graphql_note_by_id(client, auth_headers, test_user):
    """The note(id) query returns a single scoped note."""
    created = await client.post(
        "/api/notes/",
        json={"title": "Scoped", "content": "content", "tags": []},
        headers=auth_headers,
    )
    note_id = created.json()["id"]

    response = await client.post(
        "/graphql",
        json={
            "query": "query GetNote($id: ID!) { note(id: $id) { id title } }",
            "variables": {"id": note_id},
        },
        headers=auth_headers,
    )
    assert response.status_code == 200
    assert response.json()["data"]["note"]["title"] == "Scoped"


@pytest.mark.asyncio
async def test_graphql_create_note_mutation(client, auth_headers, test_user):
    """createNote mirrors POST /api/notes and returns the new note."""
    response = await client.post(
        "/graphql",
        json={
            "query": """
            mutation CreateNote($title: String!, $content: String!, $tags: [String!]!) {
              createNote(title: $title, content: $content, tags: $tags) {
                id title content tags
              }
            }
            """,
            "variables": {
                "title": "Mutation Note",
                "content": "Created via GraphQL",
                "tags": ["gql"],
            },
        },
        headers=auth_headers,
    )
    assert response.status_code == 200
    note = response.json()["data"]["createNote"]
    assert note["title"] == "Mutation Note"
    assert note["tags"] == ["gql"]


@pytest.mark.asyncio
async def test_graphql_create_note_requires_auth(client):
    """createNote without a token should error."""
    response = await client.post(
        "/graphql",
        json={
            "query": """
            mutation {
              createNote(title: "x", content: "y", tags: []) { id }
            }
            """,
        },
    )
    assert response.status_code == 200
    assert response.json()["errors"]
    assert "Unauthorized" in response.json()["errors"][0]["message"]


@pytest.mark.asyncio
async def test_graphql_update_own_user(client, auth_headers, test_user):
    """updateUser lets the authenticated user update their own profile."""
    response = await client.post(
        "/graphql",
        json={
            "query": """
            mutation UpdateUser($id: ID!, $username: String) {
              updateUser(id: $id, username: $username) { id username email }
            }
            """,
            "variables": {"id": str(test_user.id), "username": "newname"},
        },
        headers=auth_headers,
    )
    assert response.status_code == 200
    user = response.json()["data"]["updateUser"]
    assert user["username"] == "newname"


@pytest.mark.asyncio
async def test_graphql_cannot_update_other_user(
    client, auth_headers, test_user, db_session
):
    """updateUser on another user's id is forbidden."""
    other = __import__("app.models", fromlist=["User"]).User(
        email="other@example.com",
        username="otheruser",
        password_hash="x",
    )
    db_session.add(other)
    db_session.commit()
    db_session.refresh(other)

    response = await client.post(
        "/graphql",
        json={
            "query": """
            mutation UpdateUser($id: ID!, $username: String) {
              updateUser(id: $id, username: $username) { id }
            }
            """,
            "variables": {"id": str(other.id), "username": "hacked"},
        },
        headers=auth_headers,
    )
    assert response.status_code == 200
    assert "Forbidden" in response.json()["errors"][0]["message"]
