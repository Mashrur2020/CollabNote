"""Event publisher helpers - bridges API to Kafka.

A single long-lived background asyncio task drains an in-memory queue and
publishes events through one AIOKafkaProducer, all bound to the FastAPI
event loop. Started/stopped via start_publisher/stop_publisher from the
FastAPI lifespan.
"""
import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

from aiokafka import AIOKafkaProducer

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "colabnote_events")
QUEUE_MAX_SIZE = int(os.getenv("EVENT_QUEUE_MAX_SIZE", "10000"))

_producer: Optional[AIOKafkaProducer] = None
_queue: Optional[asyncio.Queue] = None
_worker_task: Optional[asyncio.Task] = None


def _serialize(value: dict) -> bytes:
    return json.dumps(value, default=str).encode("utf-8")


async def start_publisher() -> None:
    """Start the background publisher task. Idempotent."""
    global _producer, _queue, _worker_task

    if _worker_task is not None:
        return

    _queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
    _producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=_serialize,
    )
    try:
        await _producer.start()
    except Exception as e:
        logger.warning("Kafka producer failed to start: %s", e)
        _producer = None

    _worker_task = asyncio.create_task(_worker(), name="kafka-event-publisher")
    logger.info("Kafka event publisher started (topic=%s)", KAFKA_TOPIC)


async def stop_publisher() -> None:
    """Stop the background publisher task and the producer."""
    global _producer, _queue, _worker_task

    if _worker_task is not None:
        _worker_task.cancel()
        try:
            await _worker_task
        except (asyncio.CancelledError, Exception):
            pass
        _worker_task = None

    if _queue is not None:
        while not _queue.empty():
            try:
                _queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        _queue = None

    if _producer is not None:
        try:
            await _producer.stop()
        except Exception as e:
            logger.warning("Kafka producer stop failed: %s", e)
        _producer = None


async def _worker() -> None:
    """Drain the queue and publish events to Kafka."""
    assert _queue is not None
    while True:
        try:
            event = await _queue.get()
        except asyncio.CancelledError:
            break

        if _producer is None:
            continue

        try:
            await _producer.send(KAFKA_TOPIC, value=event)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning("Failed to publish event %s: %s", event.get("event_type"), e)


def publish_event(
    event_type: str,
    user_id: int,
    resource_id: Optional[str] = None,
    metadata: Optional[dict] = None,
) -> None:
    """Enqueue an event for asynchronous publication (non-blocking)."""
    if _queue is None:
        return

    event = {
        "event_type": event_type,
        "user_id": user_id,
        "resource_id": resource_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "metadata": metadata or {},
    }
    try:
        _queue.put_nowait(event)
    except asyncio.QueueFull:
        logger.warning("Event queue full; dropping event %s", event_type)


def log_user_signup(user_id: int, email: str) -> None:
    publish_event(
        event_type="user_signup",
        user_id=user_id,
        metadata={"email": email},
    )


def log_user_login(user_id: int, email: str) -> None:
    publish_event(
        event_type="user_login",
        user_id=user_id,
        metadata={"email": email},
    )


def log_note_created(
    user_id: int, note_id: str, title: str, tags: Optional[list] = None
) -> None:
    publish_event(
        event_type="note_created",
        user_id=user_id,
        resource_id=note_id,
        metadata={"title": title, "tags": tags or []},
    )


def log_note_updated(
    user_id: int,
    note_id: str,
    title: str,
    changed_fields: Optional[list] = None,
) -> None:
    publish_event(
        event_type="note_updated",
        user_id=user_id,
        resource_id=note_id,
        metadata={"title": title, "changed_fields": changed_fields or []},
    )


def log_note_deleted(user_id: int, note_id: str) -> None:
    publish_event(
        event_type="note_deleted",
        user_id=user_id,
        resource_id=note_id,
        metadata={"note_id": note_id},
    )


def log_note_searched(user_id: int, query: str, results_count: int) -> None:
    publish_event(
        event_type="note_searched",
        user_id=user_id,
        metadata={"query": query, "results_count": results_count},
    )
