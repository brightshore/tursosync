---
type: fix
"TursoSync"
---

Statements on a synced database no longer fail with "database is locked" while a checkpoint runs, including inside a transaction holding the write lock. They wait for the checkpoint to finish, as they already do for a pull's apply, and a checkpoint waits only for statements already running. This wait isn't limited by the busy timeout. A checkpoint that finds a transaction open can still fail busy, and a stale snapshot still fails at once, so transactions that hit one must still roll back and retry.
