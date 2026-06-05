# Error Log

Every real error hit during development of this project.
Format: date, error, root cause, fix, lesson learned.

---

## How To Read This

Each entry is a real problem encountered during the build.
These are not hypothetical. They happened, they were diagnosed, 
and they were fixed. This log exists because debugging is 
where the real learning happens.

---

<!-- entries will be added here as errors occur -->
## Error 001 — Zookeeper healthcheck failure on startup

**Date:** 22 May 2026

**Error:**
dependency failed to start: container zookeeper is unhealthy

**Symptom:**
Zookeeper container started but was marked unhealthy by Docker.
Kafka refused to start because it depends on zookeeper being healthy.
All other services started fine.

**Root cause:**
Usually, Docker sends the message "ruok" (Are you OK?). A normal ZooKeeper will text back "imok" (I'm OK).

But this specific version of ZooKeeper had strict security rules turned on. It was set to ignore the "ruok" message. The only message it was allowed to answer was "srvr" (Server status).

So, this is what happened:

Docker texted: "Are you OK?"

ZooKeeper said nothing (because that message was blocked).

Docker thought: Oh it didn't answer. It must be dead

Docker marked it as broken, even though ZooKeeper was actually working perfectly fine.

**Fix applied:**
Healthcheck was sending 'ruok' which is disabled in the Confluent
Zookeeper image by default. Only 'srvr' is whitelisted. Switched
healthcheck to send 'srvr' and grep response for 'Mode: standalone'
which confirms the server is genuinely ready. Added start_period of
120s to prevent false failures during slow initialisation.

**Lesson:**
Don't assume all software works the exact same way. Just because the standard version answers to "Are you OK?", it doesn't mean this specific, locked-down version will Always check the rules first.

## Checkpoint 001 — Database layer tests passing

**Date:** 4 June 2026

**What passed:**
All 5 repository tests green. Idempotency confirmed working.
The same event sent twice produces exactly one row in the database.
DLQ write confirmed working.

**What this proves:**
ON CONFLICT on event_id prevents duplicate financial records
even when Kafka delivers the same message more than once.