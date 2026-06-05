import uuid
import json
import pytest
from database.repository import save_event, save_to_dlq, get_event_count, get_dlq_count


def make_test_event(event_id=None):
    """
    Creates a fake financial event for testing.
    Every field matches exactly what a real Kafka consumer
    would pass to save_event() in production.
    """
    return {
        "event_id": str(event_id or uuid.uuid4()),
        "event_type": "TRANSFER",
        "account_id": "ACC-TEST-001",
        "amount": "250.00",
        "currency": "USD",
        "status": "processed",
        "payload": json.dumps({"note": "test transfer", "reference": "REF-001"}),
        "kafka_topic": "financial_events",
        "kafka_partition": 0,
        "kafka_offset": 1,
    }


def test_save_event_succeeds():
    """Basic check: saving a valid event returns True."""
    event = make_test_event()
    result = save_event(event)
    assert result is True


def test_save_event_idempotent():
    """
    The most important test in this file.
    Save the exact same event twice with the same event_id.
    The second save must succeed without raising an error
    and without creating a duplicate row.
    This proves our ON CONFLICT logic is working.
    """
    event_id = uuid.uuid4()
    event = make_test_event(event_id)

    first_count = get_event_count()

    save_event(event)
    save_event(event)  # exact same event sent again

    second_count = get_event_count()

    # Only one new row should exist despite two save calls
    assert second_count - first_count == 1


def test_save_to_dlq_succeeds():
    """Check that a failed event can be parked in the DLQ."""
    event = make_test_event()
    event["kafka_offset"] = 999

    result = save_to_dlq(
        event,
        failure_reason="Simulated processing failure for test",
        retry_count=3
    )
    assert result is True


def test_get_event_count_returns_integer():
    """Confirm the count function returns a usable number."""
    count = get_event_count()
    assert isinstance(count, int)
    assert count >= 0


def test_get_dlq_count_returns_integer():
    """Confirm the DLQ count function returns a usable number."""
    count = get_dlq_count()
    assert isinstance(count, int)
    assert count >= 0