# Financial Intelligence System

A real-time financial event pipeline that processes 10,000+ events per minute 
with at-least-once delivery guarantees and zero message loss or duplication.

Built to understand how production financial systems handle high-throughput 
event processing reliably — the kind of problem where a single duplicate 
transaction or lost event means real money affected.

---

## What it does

Financial events (transfers, deposits, withdrawals, fraud alerts) are 
published to Kafka by a producer and consumed by a processing layer that 
writes them to PostgreSQL. The system guarantees that no event is ever lost 
and no event is ever processed twice, even when components fail mid-operation.

---

## Architecture

Producer → Kafka (3 partitions) → Consumer Group → PostgreSQL
↓
Redis (dedup cache)
↓
Dead Letter Queue (on failure)
↓
Prometheus → Grafana

---

## Measured results

All numbers below were measured under real load. Screenshots in docs/screenshots/.

| Metric | Result | How measured |
|---|---|---|
| Throughput | 10,498 events/min | Producer rate log over 65 second run |
| p99 latency | <75ms | Prometheus histogram: 21,133/21,175 events under 75ms |
| Consumer lag | 0 | Kafka consumer groups after full test run |
| DLQ failures | 0 | Grafana DLQ panel during load test |
| Idempotency | Proven | 20 messages in, 10 rows created |

---

## Key technical decisions

**At-least-once delivery with idempotent writes, not exactly-once**

Kafka supports exactly-once via transactional producers but adds roughly 
15-20ms latency overhead per message. For this system idempotent PostgreSQL 
upserts using event_id as a unique constraint give the same correctness 
guarantee at lower cost. If regulatory requirements demanded exactly-once 
at the broker level that tradeoff would flip.

**Manual offset commits, not automatic**

Automatic commits advance the offset on a timer regardless of whether 
processing succeeded. A crash between the auto-commit and the database 
write loses the message permanently. Manual commits only advance after 
confirmed PostgreSQL write. Nothing is ever permanently lost.

**Dead letter queue after 3 retries with exponential backoff**

A failing message that retries forever blocks the entire partition. After 
3 attempts (waiting 1s, 2s, 4s between each) the message is routed to a 
separate DLQ topic and the partition continues. The failure is recorded 
with the full error reason for investigation and replay.

**Connection pooling with min 2 max 10 connections**

Opening a new database connection for every event at 10,000 events per 
minute would add significant latency and overwhelm PostgreSQL. A connection 
pool keeps connections open and ready. Borrowing an existing connection 
costs microseconds. Opening a new one costs milliseconds.

---

## What production would add

- Kafka transactional producers for exactly-once at the broker level
- Avro schema registry for event schema versioning and evolution
- Hash-chaining on the event log for tamper evidence
- Reconciliation job to catch any drift between Kafka and PostgreSQL
- Redis Cluster instead of standalone Redis
- Multiple Kafka brokers with replication factor 3

---

## Running locally

Requirements: Docker Desktop, Python 3.10+

```bash
git clone https://github.com/bigfehd/financial-intelligence-system.git
cd financial-intelligence-system

# start all infrastructure
docker compose up -d

# install dependencies
python -m venv venv
venv\Scripts\activate
pip install -e .
pip install psycopg2-binary python-dotenv kafka-python prometheus-client pytest

# create .env file with database credentials
# see .env.example for the required variables

# run database migrations
# on Windows:
Get-Content database\migrations\001_initial.sql | docker exec -i postgres psql -U financeuser -d financedb

# start the consumer
python -m consumer.kafka_consumer

# in a second terminal, run the producer
python -m producer.kafka_producer
```

---

## Decisions and error log

- `docs/decisions/` — architecture decision records for every major tradeoff
- `docs/errors.md` — every real error hit during the build and how it was fixed
- `docs/screenshots/` — evidence for every metric claimed above