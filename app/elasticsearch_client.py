"""Elasticsearch client for full-text search in ColabNote."""
import os
from dotenv import load_dotenv
from elasticsearch import AsyncElasticsearch

load_dotenv()

ES_URL = os.getenv("ES_URL", "http://localhost:9200")
INDEX_NAME = os.getenv("ELASTICSEARCH_INDEX", "notes")

# Async Elasticsearch client
es_client = AsyncElasticsearch(
    hosts=[ES_URL],
    verify_certs=False,
)

INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "title": {"type": "text", "analyzer": "standard"},
            "content": {"type": "text", "analyzer": "standard"},
            "user_email": {"type": "keyword"},
            "tags": {"type": "keyword"},
            "created_at": {"type": "date"},
            "note_id": {"type": "keyword"},
        }
    }
}


async def create_index():
    """Create the notes index if it doesn't exist."""
    try:
        exists = await es_client.indices.exists(index=INDEX_NAME)
        if not exists:
            await es_client.indices.create(index=INDEX_NAME, body=INDEX_MAPPING)
            print(f" Elasticsearch index '{INDEX_NAME}' created")
        else:
            print(f" Elasticsearch index '{INDEX_NAME}' already exists")
    except Exception as e:
        print(f"  Elasticsearch index creation skipped: {e}")


async def index_note(
    note_id: str,
    title: str,
    content: str,
    user_email: str,
    created_at: str,
    tags: list[str] | None = None,
):
    """Index a note in Elasticsearch."""
    try:
        await es_client.index(
            index=INDEX_NAME,
            id=note_id,
            document={
                "title": title,
                "content": content,
                "user_email": user_email,
                "tags": tags or [],
                "created_at": created_at,
                "note_id": note_id,
            },
        )
    except Exception as e:
        print(f" Failed to index note: {e}")


async def update_note_index(
    note_id: str,
    title: str | None = None,
    content: str | None = None,
    tags: list[str] | None = None,
):
    """Update a note in Elasticsearch."""
    try:
        doc: dict = {}
        if title is not None:
            doc["title"] = title
        if content is not None:
            doc["content"] = content
        if tags is not None:
            doc["tags"] = tags
        if doc:
            await es_client.update(
                index=INDEX_NAME,
                id=note_id,
                doc=doc,
            )
    except Exception as e:
        print(f"  Failed to update note in ES: {e}")


async def delete_note_index(note_id: str):
    """Remove a note from Elasticsearch."""
    try:
        await es_client.delete(index=INDEX_NAME, id=note_id)
    except Exception as e:
        print(f"  Failed to delete note from ES: {e}")


async def search_notes(query: str, user_email: str, limit: int = 10):
    """Search notes by query for a specific user with fuzzy matching."""
    try:
        # Refresh index to ensure latest docs are searchable
        await es_client.indices.refresh(index=INDEX_NAME)
        
        result = await es_client.search(
            index=INDEX_NAME,
            body={
                "query": {
                    "bool": {
                        "must": [
                            {
                                "multi_match": {
                                    "query": query,
                                    "fields": ["title^3", "content"],
                                    "fuzziness": "AUTO",
                                }
                            }
                        ],
                        "filter": [{"term": {"user_email": user_email}}],
                    }
                },
                "highlight": {
                    "fields": {
                        "title": {},
                        "content": {"fragment_size": 150, "number_of_fragments": 3},
                    }
                },
                "size": limit,
            },
        )
        
        hits = []
        for hit in result["hits"]["hits"]:
            item = {"id": hit["_id"], "score": hit["_score"], **hit["_source"]}
            if "highlight" in hit:
                item["highlight"] = hit["highlight"]
            hits.append(item)
        
        return {"total": result["hits"]["total"]["value"], "results": hits}
    except Exception as e:
        print(f"  Search failed: {e}")
        return {"total": 0, "results": []}


async def close_es():
    """Close Elasticsearch connection."""
    await es_client.close()