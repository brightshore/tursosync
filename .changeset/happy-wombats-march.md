---
type: fix
"TursoSync"
---

A statement that finds the database locked while it is being prepared now waits for the lock instead of failing at once, as long as the connection is not inside a transaction. The engine's busy timeout only ever covered running a statement, not preparing it, so a statement that arrived during a checkpoint failed with "prepare_single: database is locked" in microseconds, whatever busy timeout was set. It now retries until the connection's busy timeout runs out, as opening a connection already did. Inside a transaction a busy prepare still fails straight away, because the transaction's snapshot may be stale and only rolling it back and starting again can help.