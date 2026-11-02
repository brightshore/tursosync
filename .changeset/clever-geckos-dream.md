---
type: fix
"TursoSync"
---

A BEGIN IMMEDIATE that fails busy no longer leaves its connection stuck in a transaction holding the write lock, which could happen under load and blocked other writers until the connection closed. Disposing a transaction whose rollback fails with no transaction left no longer closes the connection.