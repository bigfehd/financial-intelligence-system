import os
import json
import uuid
import time
import random
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
logger = logging.getLogger(__name__)

# The types of financial events our system processes.
# In a real bank these would come from actual banking systems.
# We generate them synthetically to simulate real load.
EVENT_TYPES = [
    "TRANSFER",
    "DEPOSIT",
    "WITHDRAWAL",
    "FRAUD_ALERT",
    "BALANCE_CHECK",
]

# Simulated account IDs.
# We use a fixed pool of accounts so we can later verify
# that balance calculations are correct per account.
# If every event had a random account ID we could never
# validate correctness meaningfully.
ACCOUNT_IDS = [f"ACC-{str(i).zfill(4)}" for i in range(1, 51)]


def create_topic_if_not_exists(topic_name: str, num_partitions: int):
    """
    Creates a Kafka topic with the specified number of partitions.
    
    We create topics explicitly instead of letting Kafka auto-create them.
    Auto-creation is disabled in our docker-compose.yml because a typo
    in a topic name would silently create a wrong topic and events would
    disappear into it with no error raised. Explicit creation means we
    control the partition count and catch naming mistakes immediately.
    
    replication_factor=1 because we run a single broker locally.
    In production with multiple brokers this would be 3.
    """
    admin = KafkaAdminClient(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    try:
        admin.create_topics([
            NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=1
            )
        ])
        logger.info("Created topic: %s with %d partitions", topic_name, num_partitions)
    except TopicAlreadyExistsError:
        logger.info("Topic already exists: %s", topic_name)
    finally:
        admin.close()


def make_event(event_type: str = None) -> dict:
    """
    Generates a single synthetic financial event.
    
    Every event gets a UUID as its event_id. This UUID is the
    deduplication key on the PostgreSQL side. It is generated here
    by the producer and travels with the event through Kafka
    all the way to the database. If Kafka delivers this event
    twice, the same UUID arrives twice, and the database
    ON CONFLICT clause handles it safely.
    """
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type or random.choice(EVENT_TYPES),
        "account_id": random.choice(ACCOUNT_IDS),
        "amount": str(round(random.uniform(1.00, 10000.00), 2)),
        "currency": "USD",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "metadata": {
            "source": "synthetic_producer",
            "version": "1.0"
        }
    }


def get_producer() -> KafkaProducer:
    """
    Creates and returns a configured Kafka producer.
    
    key_serializer: We use the account_id as the partition key.
    This ensures all events for the same account always go to
    the same partition, which means they are processed in order
    for that account. Without this, events for ACC-0001 could
    land in partition 0 on one send and partition 2 on the next,
    and be processed out of order.
    
    value_serializer: Converts the Python dict to JSON bytes
    before sending. Kafka stores raw bytes, not Python objects.
    
    acks=all: The broker only confirms receipt after all
    in-sync replicas have written the message. With a single
    broker locally this is effectively acks=1, but in production
    this prevents data loss if the leader broker fails immediately
    after acknowledging.
    
    retries=3: If the send fails, retry up to 3 times before
    raising an error. Handles transient network issues.
    """
    return KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
        retry_backoff_ms=100,
    )


def run_producer(events_per_second: int = 170, duration_seconds: int = 60):
    """
    Sends financial events to Kafka continuously for a set duration.
    
    Default rate: 170 events per second = ~10,200 events per minute.
    This hits our 10,000 events/min target with a small buffer.
    
    We sleep between batches to control the rate precisely.
    Without the sleep we would flood Kafka as fast as the network
    allows which would make throughput measurement meaningless.
    
    The partition key is the account_id. This means:
    - All events for ACC-0001 always go to the same partition
    - Events within one account are processed in arrival order
    - Load is distributed across partitions by account
    """
    topic = os.getenv("KAFKA_TOPIC", "financial_events")
    num_partitions = int(os.getenv("KAFKA_NUM_PARTITIONS", 3))

    # Create topics before producing anything
    create_topic_if_not_exists(topic, num_partitions)
    create_topic_if_not_exists(
        os.getenv("KAFKA_DLQ_TOPIC", "financial_events_dlq"),
        num_partitions=1
    )

    producer = get_producer()

    logger.info(
        "Producer starting: %d events/sec for %d seconds (target: ~%d events/min)",
        events_per_second,
        duration_seconds,
        events_per_second * 60
    )

    total_sent = 0
    start_time = time.time()
    batch_size = events_per_second
    sleep_interval = 1.0

    try:
        while time.time() - start_time < duration_seconds:
            batch_start = time.time()

            for _ in range(batch_size):
                event = make_event()
                producer.send(
                    topic=topic,
                    key=event["account_id"],
                    value=event
                )
                total_sent += 1

            # Flush ensures all buffered messages are sent to Kafka
            # before we sleep. Without flush, messages sit in the
            # producer buffer and may not be sent immediately.
            producer.flush()

            elapsed = time.time() - batch_start
            sleep_time = max(0, sleep_interval - elapsed)
            time.sleep(sleep_time)

            logger.info(
                "Sent %d events | Total: %d | Elapsed: %.1fs",
                batch_size,
                total_sent,
                time.time() - start_time
            )

    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
    finally:
        producer.flush()
        producer.close()
        logger.info(
            "Producer finished. Total events sent: %d in %.1f seconds",
            total_sent,
            time.time() - start_time
        )


if __name__ == "__main__":
    run_producer()