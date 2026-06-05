import logging
from database.connection import get_connection, return_connection

logger = logging.getLogger(__name__)


def save_event(event: dict) -> bool:
    """
    Saves a financial event to the database using an idempotent upsert.

    Idempotent means running this function once or ten times with the
    same event produces exactly the same result. No duplicates ever.

    How it works:
    INSERT the event normally. If PostgreSQL sees that event_id already
    exists it hits the UNIQUE constraint. Instead of raising an error,
    ON CONFLICT tells PostgreSQL to update the existing row instead.
    The result is identical whether this is the first time we saw this
    event or the tenth time.

    This is what protects us from Kafka's at-least-once delivery.
    Kafka may send the same event more than once. This function
    makes that safe.

    Returns True if the event was saved, False if something went wrong.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO events (
                    event_id,
                    event_type,
                    account_id,
                    amount,
                    currency,
                    status,
                    payload,
                    kafka_topic,
                    kafka_partition,
                    kafka_offset
                ) VALUES (
                    %(event_id)s,
                    %(event_type)s,
                    %(account_id)s,
                    %(amount)s,
                    %(currency)s,
                    %(status)s,
                    %(payload)s,
                    %(kafka_topic)s,
                    %(kafka_partition)s,
                    %(kafka_offset)s
                )
                ON CONFLICT (event_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    processed_at = NOW()
                """,
                {
                    "event_id": event["event_id"],
                    "event_type": event["event_type"],
                    "account_id": event["account_id"],
                    "amount": event["amount"],
                    "currency": event.get("currency", "USD"),
                    "status": event.get("status", "processed"),
                    "payload": event["payload"],
                    "kafka_topic": event["kafka_topic"],
                    "kafka_partition": event["kafka_partition"],
                    "kafka_offset": event["kafka_offset"],
                },
            )
        conn.commit()
        logger.info(
            "Event saved: event_id=%s account_id=%s type=%s",
            event["event_id"],
            event["account_id"],
            event["event_type"],
        )
        return True

    except Exception as e:
        # If anything goes wrong we roll back the transaction.
        # A rolled back transaction leaves the database exactly
        # as it was before we started. No partial writes, no corruption.
        conn.rollback()
        logger.error(
            "Failed to save event: event_id=%s error=%s",
            event.get("event_id"),
            str(e),
        )
        return False

    finally:
        # This runs whether the save succeeded or failed.
        # Returning the connection to the pool is non-negotiable.
        # If we skip this the pool runs out of connections
        # and the entire system stops writing to the database.
        return_connection(conn)


def save_to_dlq(event: dict, failure_reason: str, retry_count: int) -> bool:
    """
    Saves a failed event to the dead letter queue.

    Called when an event has failed processing 3 times in a row.
    Instead of dropping it or blocking the Kafka partition forever,
    we park it here safely so it can be investigated and replayed later.

    Every failure is recorded with the exact reason it failed.
    Nothing is ever silently lost.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dead_letter_queue (
                    event_id,
                    event_type,
                    account_id,
                    payload,
                    kafka_topic,
                    kafka_partition,
                    kafka_offset,
                    failure_reason,
                    retry_count
                ) VALUES (
                    %(event_id)s,
                    %(event_type)s,
                    %(account_id)s,
                    %(payload)s,
                    %(kafka_topic)s,
                    %(kafka_partition)s,
                    %(kafka_offset)s,
                    %(failure_reason)s,
                    %(retry_count)s
                )
                """,
                {
                    "event_id": event.get("event_id"),
                    "event_type": event.get("event_type"),
                    "account_id": event.get("account_id"),
                    "payload": event.get("payload"),
                    "kafka_topic": event["kafka_topic"],
                    "kafka_partition": event["kafka_partition"],
                    "kafka_offset": event["kafka_offset"],
                    "failure_reason": failure_reason,
                    "retry_count": retry_count,
                },
            )
        conn.commit()
        logger.warning(
            "Event sent to DLQ: event_id=%s reason=%s retries=%d",
            event.get("event_id"),
            failure_reason,
            retry_count,
        )
        return True

    except Exception as e:
        conn.rollback()
        logger.error(
            "Failed to save to DLQ: event_id=%s error=%s",
            event.get("event_id"),
            str(e),
        )
        return False

    finally:
        return_connection(conn)


def get_event_count() -> int:
    """
    Returns the total number of events in the database.
    Used during load testing to verify events are being written
    and during the replay correctness test to compare run counts.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM events")
            return cur.fetchone()[0]
    except Exception as e:
        logger.error("Failed to get event count: %s", str(e))
        return -1
    finally:
        return_connection(conn)


def get_dlq_count() -> int:
    """
    Returns the total number of events in the dead letter queue.
    Exposed as a Prometheus metric so we can alert when this number
    rises unexpectedly, which indicates a systemic processing failure.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM dead_letter_queue WHERE resolved = FALSE")
            return cur.fetchone()[0]
    except Exception as e:
        logger.error("Failed to get DLQ count: %s", str(e))
        return -1
    finally:
        return_connection(conn)