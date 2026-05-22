# ADR 001: At-Least-Once vs Exactly-Once Delivery

## Date
May-22-2025

## Context
Kafka offers different delivery guarantees depending on configuration.
The choice affects correctness, latency, and complexity of the consumer code.
For a financial event pipeline the delivery guarantee is the most 
consequential architectural decision.

## Options Considered

### Option 1: Exactly-Once (Kafka Transactional Producers)
Kafka supports exactly-once semantics via transactional producers and 
the `enable.idempotence=true` producer config combined with 
`isolation.level=read_committed` on the consumer.

**Pros:**
- No duplicate messages ever reach the consumer
- Simpler consumer code — no deduplication logic needed

**Cons:**
- ~15-20ms additional latency per message due to transaction coordination
- Significantly more complex producer and consumer configuration
- Overkill when idempotent writes at the consumer provide equivalent correctness

### Option 2: At-Least-Once with Idempotent Consumer (Chosen)
Use default Kafka delivery with manual offset commits on the consumer side.
Accept that duplicates may arrive. Handle them with idempotent PostgreSQL 
upserts using event ID as deduplication key.

**Pros:**
- Lower latency — no transaction coordination overhead
- Simpler Kafka configuration
- Idempotent upsert provides equivalent correctness guarantee at the DB layer
- More resilient — consumer can replay without side effects

**Cons:**
- Consumer code must handle duplicates explicitly
- Requires careful offset commit management to avoid message loss

## Decision
At-least-once delivery with idempotent PostgreSQL upserts.

## Consequences
Every event must carry a globally unique event ID.
The PostgreSQL upsert must use ON CONFLICT on that event ID.
Offset commits must happen only after confirmed DB write.
Consumer crash recovery must be tested explicitly.

## What Production Would Change
In a regulatory environment where audit requirements mandate 
exactly-once guarantees at the broker level, we would enable 
Kafka transactional producers and accept the latency tradeoff.