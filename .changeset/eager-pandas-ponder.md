---
type: fix
"TursoSync"
---

Upserts on sync replicas no longer fail with invalid text or blob errors while capturing changes. Change capture keeps the actual previous row so replay and rollback can restore it correctly.