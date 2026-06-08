from prometheus_client import Counter, Histogram, Gauge, start_http_server
import os
import logging

logger = logging.getLogger(__name__)

# A Counter only goes up. It never decreases.
# It resets to zero when the application restarts.
# Perfect for tracking total events processed over time.
# In Grafana you use rate(events_processed_total[1m]) to see
# how many events per second are being processed right now.
events_processed_total = Counter(
    "events_processed_total",
    "Total number of financial events successfully processed and saved to PostgreSQL",
    ["event_type", "account_id_prefix"]
)

# Separate counter for failures so we can track failure rate
# independently from success rate.
events_failed_total = Counter(
    "events_failed_total",
    "Total number of events that failed processing after all retries",
    ["failure_reason"]
)

# A Histogram tracks the distribution of values over time.
# Every time you record a value it gets sorted into buckets.
# Buckets here represent latency thresholds in seconds.
# The 0.12 bucket is the one that proves your <120ms p99 claim.
# If 99% of observations fall below 0.12, your p99 is under 120ms.
event_processing_duration = Histogram(
    "event_processing_duration_seconds",
    "Time taken to process a single event from Kafka receipt to PostgreSQL write confirmation",
    buckets=[0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.12, 0.15, 0.2, 0.3, 0.5, 1.0]
)

# Counter for DLQ entries. When this rises unexpectedly
# it means something systemic is failing, not just one bad event.
dlq_messages_total = Counter(
    "dlq_messages_total",
    "Total number of events routed to the dead letter queue after exhausting retries"
)

# A Gauge can go up and down. Perfect for consumer lag
# because lag increases when producer is faster than consumer
# and decreases when consumer catches up.
# We update this manually after each poll.
consumer_lag = Gauge(
    "kafka_consumer_lag",
    "Number of messages waiting in Kafka that the consumer has not yet processed",
    ["partition"]
)

# Gauge for tracking active database connections
# so we can see if the connection pool is under pressure.
db_connections_active = Gauge(
    "db_connections_active",
    "Number of currently active PostgreSQL connections from the pool"
)

# Gauge for DLQ backlog — unresolved failures waiting for attention.
dlq_unresolved = Gauge(
    "dlq_unresolved_total",
    "Number of unresolved events in the dead letter queue"
)


def start_metrics_server():
    """
    Starts the HTTP server that Prometheus scrapes.

    This exposes a /metrics endpoint on the configured port.
    Prometheus visits this endpoint every 5 seconds as configured
    in monitoring/prometheus.yml and reads all the metric values.

    Must be called once at application startup before the
    consumer loop begins. If this is not called Prometheus
    has nothing to scrape and all your dashboards show empty graphs.
    """
    port = int(os.getenv("METRICS_PORT", 8000))
    start_http_server(port)
    logger.info("Prometheus metrics server started on port %d", port)