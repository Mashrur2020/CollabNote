"""
Standalone Kafka producer throughput benchmark for ColabNote's event pipeline.
Publishes the same event shape as app/events.py to the real broker.

The broker advertises itself as "kafka:9092", which only resolves inside the
docker-compose network -- so run this from a container on that network
(the consumer image already has aiokafka installed):

  docker cp kafka_bench.py colabnote_consumer:/tmp/kafka_bench.py
  docker exec colabnote_consumer sed -i 's/localhost:9092/kafka:9092/' /tmp/kafka_bench.py
  docker exec colabnote_consumer python /tmp/kafka_bench.py
"""
import asyncio
import json
import time
import uuid
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer

BOOTSTRAP = "localhost:9092"
TOPIC = "colabnote_events"
NUM_EVENTS = 20000
BATCH_SIZE = 500


def make_event(i: int) -> dict:
    return {
        "event_type": "note_created",
        "user_id": i % 1000,
        "resource_id": uuid.uuid4().hex,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "metadata": {"title": f"Note {i}", "tags": ["bench", "load"]},
    }


async def main():
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )
    await producer.start()
    try:
        start = time.perf_counter()
        for batch_start in range(0, NUM_EVENTS, BATCH_SIZE):
            batch = [
                producer.send(TOPIC, value=make_event(i))
                for i in range(batch_start, min(batch_start + BATCH_SIZE, NUM_EVENTS))
            ]
            await asyncio.gather(*batch)
        await producer.flush()
        elapsed = time.perf_counter() - start

        print(f"Published {NUM_EVENTS} events in {elapsed:.2f}s")
        print(f"Throughput: {NUM_EVENTS / elapsed:.1f} events/sec")
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
