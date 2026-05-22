# Financial Intelligence System

A real-time financial event pipeline built to process 10,000+ events per minute 
with at-least-once delivery guarantees and zero message loss or duplication.

## Status

Active development — building in public. Follow the commit history to see 
real progress, real bugs, and real decisions made incrementally.

## What This Builds

A production-grade event processing system that solves a real problem in 
financial infrastructure: how do you process thousands of transactions per 
second reliably, without losing a single event, and without processing any 
event twice — even when components fail?

## Stack

- **Kafka** — distributed event streaming, at-least-once delivery
- **PostgreSQL** — source of truth, idempotent upserts
- **Redis** — fast deduplication cache
- **Prometheus + Grafana** — metrics and observability
- **Locust** — load testing and benchmark measurement

## Architecture

Coming after initial build 

## Documentation

- `docs/decisions/` — Architecture Decision Records for every major tradeoff
- `docs/errors.md` — Real errors hit during development and how they were solved
- `docs/load-test-results/` — Raw benchmark output and screenshots
- `docs/screenshots/` — Evidence for every metric claimed

## Running Locally

Coming after Docker Compose is complete.