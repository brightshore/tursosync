---
type: fix
"TursoSync"
---

Opening a connection to a synced database no longer fails with `database tape error: database is busy` when it lands during a checkpoint or another writer's lock. The open now waits up to the connection's busy timeout (5 seconds by default), the same as a statement already did, so a write that opens its own connection is no longer lost to momentary contention.