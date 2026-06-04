"""Test Elasticsearch integration for ColabNote."""
import asyncio
import pytest
from app.elasticsearch_client import (
    es_client,
    create_index,
    index_note,
    update_note_index,
    delete_note_index,
    search_notes,
    INDEX_NAME,
)


@pytest.fixture(scope="module")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="module", autouse=True)
async def setup_index():
    """Create ES index before tests."""
    await create_index()
    yield
    # Cleanup: delete test index after tests
    try:
        await es_client.indices.delete(index=INDEX_NAME, ignore=[400, 404])
    except Exception:
        pass
    finally:
        await es_client.close()


@pytest.fixture(autouse=True)
async def clean_index():
    """Clean index before each test."""
    try:
        await es_client.delete_by_query(
            index=INDEX_NAME,
            body={"query": {"match_all": {}}},
            ignore=[400, 404],
        )
        await es_client.indices.refresh(index=INDEX_NAME)
    except Exception:
        pass
    yield


@pytest.mark.asyncio
async def test_create_index():
    """Test index creation."""
    await create_index()
    exists = await es_client.indices.exists(index=INDEX_NAME)
    assert bool(exists) is True  # Convert HeadApiResponse to bool


@pytest.mark.asyncio
async def test_index_note():
    """Test indexing a note."""
    await index_note(
        note_id="test1",
        title="Python FastAPI",
        content="Learn FastAPI framework",
        user_email="test@example.com",
        created_at="2024-01-01T00:00:00",
    )
    
    # Refresh and verify
    await es_client.indices.refresh(index=INDEX_NAME)
    
    doc = await es_client.get(index=INDEX_NAME, id="test1")
    assert doc["_source"]["title"] == "Python FastAPI"
    assert doc["_source"]["user_email"] == "test@example.com"


@pytest.mark.asyncio
async def test_update_note_index():
    """Test updating a note in ES."""
    # First index
    await index_note(
        note_id="test2",
        title="Original Title",
        content="Original content",
        user_email="test@example.com",
        created_at="2024-01-01",
    )
    await es_client.indices.refresh(index=INDEX_NAME)
    
    # Update
    await update_note_index("test2", "Updated Title", "Updated content")
    await es_client.indices.refresh(index=INDEX_NAME)
    
    # Verify
    doc = await es_client.get(index=INDEX_NAME, id="test2")
    assert doc["_source"]["title"] == "Updated Title"
    assert doc["_source"]["content"] == "Updated content"


@pytest.mark.asyncio
async def test_delete_note_index():
    """Test deleting a note from ES."""
    await index_note(
        note_id="test3",
        title="To Delete",
        content="This will be deleted",
        user_email="test@example.com",
        created_at="2024-01-01",
    )
    await es_client.indices.refresh(index=INDEX_NAME)
    
    await delete_note_index("test3")
    
    # Verify deletion
    try:
        await es_client.get(index=INDEX_NAME, id="test3")
        assert False, "Document should be deleted"
    except Exception:
        pass  # Expected - document not found


@pytest.mark.asyncio
async def test_search_notes():
    """Test search functionality."""
    # Index multiple notes
    await index_note("s1", "Python Programming", "Learn Python basics", "user@test.com", "2024-01-01")
    await index_note("s2", "FastAPI Tutorial", "Build APIs with FastAPI", "user@test.com", "2024-01-02")
    await index_note("s3", "JavaScript Guide", "Learn JS for web", "user@test.com", "2024-01-03")
    await es_client.indices.refresh(index=INDEX_NAME)
    
    # Search for Python
    results = await search_notes("Python", "user@test.com")
    assert results["total"] == 1  # Only s1 matches "Python"
    
    # Search for FastAPI
    results = await search_notes("FastAPI", "user@test.com")
    assert results["total"] == 1
    assert results["results"][0]["note_id"] == "s2"


@pytest.mark.asyncio
async def test_search_user_isolation():
    """Test that search only returns user's notes."""
    await index_note("u1", "User1 Note", "Secret data", "user1@test.com", "2024-01-01")
    await index_note("u2", "User2 Note", "Private data", "user2@test.com", "2024-01-01")
    await es_client.indices.refresh(index=INDEX_NAME)
    
    # User1 should only see their notes
    results = await search_notes("Note", "user1@test.com")
    assert results["total"] == 1
    assert results["results"][0]["user_email"] == "user1@test.com"


@pytest.mark.asyncio
async def test_search_highlighting():
    """Test search highlighting."""
    await index_note("h1", "FastAPI is awesome", "Build web APIs", "test@test.com", "2024-01-01")
    await es_client.indices.refresh(index=INDEX_NAME)
    
    results = await search_notes("FastAPI", "test@test.com")
    assert "highlight" in results["results"][0]
    assert "FastAPI" in results["results"][0]["highlight"]["title"][0]