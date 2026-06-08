from monitoring.metrics import (
    events_processed_total,
    events_failed_total,
    event_processing_duration,
    dlq_messages_total,
    consumer_lag,
    start_metrics_server,
)
import time
import os
import json
import time
import logging
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from database.repository import save_event, save_to_dlq
from database.connection import close_pool

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
logger = logging.getLogger(__name__)

# Maximum number of times we attempt to process a failing event
# before giving up and routing it to the dead letter queue.
# Three attempts with exponential backoff gives transient failures
# (network blips, brief DB unavailability) time to resolve
# without blocking the partition for too long.
MAX_RETRIES = 3

# Base delay in seconds for exponential backoff.
# Attempt 1 waits 1 second, attempt 2 waits 2 seconds,
# attempt 3 waits 4 seconds. Total max wait: 7 seconds.
# After that the event is a poison pill and goes to the DLQ.
RETRY_BASE_DELAY = 1


def process_message(message) -> bool:
    """
    Processes a single Kafka message from start to finish.
    """
    start = time.time()

    try:
        event = json.loads(message.value.decode("utf-8"))

        event["kafka_topic"] = message.topic
        event["kafka_partition"] = message.partition
        event["kafka_offset"] = message.offset
        event["payload"] = json.dumps(event)

        success = save_event(event)

        duration = time.time() - start

        if success:
            # Record the processing duration in the histogram.
            # Every call to observe() adds one data point.
            # Prometheus aggregates these into percentiles.
            event_processing_duration.observe(duration)

            # Increment the success counter with labels.
            # Labels let you break down the metric by event type
            # so you can see how many TRANSFER vs DEPOSIT events
            # were processed in a given time window.
            events_processed_total.labels(
                event_type=event.get("event_type", "unknown"),
                account_id_prefix=event.get("account_id", "unknown")[:4]
            ).inc()

        return success

    except json.JSONDecodeError as e:
        logger.error(
            "Failed to deserialise message at partition=%d offset=%d: %s",
            message.partition,
            message.offset,
            str(e)
        )
        events_failed_total.labels(failure_reason="json_decode_error").inc()
        return False

    except Exception as e:
        logger.error(
            "Unexpected error processing message at partition=%d offset=%d: %s",
            message.partition,
            message.offset,
            str(e)
        )
        events_failed_total.labels(failure_reason="unexpected_error").inc()
        return False


def process_with_retry(message, consumer) -> bool:
    """
    Attempts to process a message up to MAX_RETRIES times.

    On each failure we wait with exponential backoff before retrying.
    Exponential backoff means we wait longer between each attempt.
    This gives temporary problems time to resolve themselves without
    hammering the database with rapid retries.

    Wait times: 1s, 2s, 4s before final failure.

    If all retries fail the message goes to the dead letter queue.
    The offset is committed regardless so the partition keeps moving.
    A stuck offset would cause the consumer to replay the same
    failing message forever, blocking all subsequent messages
    in that partition.
    """
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        success = process_message(message)

        if success:
            if attempt > 1:
                logger.info(
                    "Message succeeded on attempt %d: partition=%d offset=%d",
                    attempt,
                    message.partition,
                    message.offset
                )
            return True

        # Processing failed. Log it and decide whether to retry.
        last_error = f"Processing failed on attempt {attempt} of {MAX_RETRIES}"
        logger.warning(
            "Attempt %d/%d failed: partition=%d offset=%d",
            attempt,
            MAX_RETRIES,
            message.partition,
            message.offset
        )

        if attempt < MAX_RETRIES:
            # Exponential backoff: 1s, 2s, 4s
            delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
            logger.info("Retrying in %ds...", delay)
            time.sleep(delay)

    # All retries exhausted. Route to dead letter queue.
    logger.error(
        "All %d attempts failed. Routing to DLQ: partition=%d offset=%d",
        MAX_RETRIES,
        message.partition,
        message.offset
    )

    try:
        event = json.loads(message.value.decode("utf-8"))
        event["kafka_topic"] = message.topic
        event["kafka_partition"] = message.partition
        event["kafka_offset"] = message.offset
        event["payload"] = json.dumps(event)
    except Exception:
        event = {
            "kafka_topic": message.topic,
            "kafka_partition": message.partition,
            "kafka_offset": message.offset,
            "payload": str(message.value),
        }

    save_to_dlq(
        event=event,
        failure_reason=last_error,
        retry_count=MAX_RETRIES
    )
    dlq_messages_total.inc()
    return False


def run_consumer():
    start_metrics_server()
    """
    Main consumer loop. Runs until interrupted.

    IMPORTANT: We use manual offset commits, not automatic.

    Automatic offset commits advance the offset on a timer
    regardless of whether processing succeeded. This means:
    - Consumer reads message at offset 5
    - Auto-commit fires, advances to offset 6
    - Consumer crashes before saving to PostgreSQL
    - On restart consumer reads from offset 6
    - Offset 5 is gone forever. The event is permanently lost.

    Manual offset commits only advance the offset after we have
    confirmed the message was either saved to PostgreSQL or
    safely parked in the DLQ. Nothing is ever permanently lost.

    enable_auto_commit=False is the key setting that enables this.
    """
    consumer = KafkaConsumer(
        os.getenv("KAFKA_TOPIC", "financial_events"),
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        group_id=os.getenv("KAFKA_CONSUMER_GROUP", "event_processors"),
        # Manual offset management
        enable_auto_commit=False,
        # Start from the earliest unprocessed message on first run.
        # After the first run the consumer group remembers its position
        # and this setting is ignored for subsequent runs.
        auto_offset_reset="earliest",
        # Deserialise the raw bytes back to Python objects
        value_deserializer=lambda x: x,
        key_deserializer=lambda x: x.decode("utf-8") if x else None,
        # How long to wait for new messages before returning an empty batch.
        # 1000ms means the consumer checks for messages every second.
        # Maximum messages per poll call.
        # 500 is a safe batch size that balances throughput
        # with memory usage and commit granularity.
        max_poll_records=500,
    )

    logger.info(
        "Consumer started. Group: %s Topic: %s",
        os.getenv("KAFKA_CONSUMER_GROUP"),
        os.getenv("KAFKA_TOPIC")
    )

    total_processed = 0
    total_failed = 0
    start_time = time.time()

    try:
        for message in consumer:
            success = process_with_retry(message, consumer)

            if success:
                total_processed += 1
            else:
                total_failed += 1

            # Commit the offset for this specific message.
            # We commit after every message, not in batches,
            # to minimise the number of messages re-processed
            # if the consumer crashes. The tradeoff is slightly
            # more commits to Kafka but much smaller replay windows.
            consumer.commit()

            if total_processed % 1000 == 0 and total_processed > 0:
                elapsed = time.time() - start_time
                rate = total_processed / elapsed * 60
                logger.info(
                    "Progress: %d processed, %d failed, %.0f events/min",
                    total_processed,
                    total_failed,
                    rate
                )

    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    finally:
        consumer.close()
        close_pool()
        logger.info(
            "Consumer finished. Total processed: %d, Total failed: %d",
            total_processed,
            total_failed
        )


if __name__ == "__main__":
    run_consumer()