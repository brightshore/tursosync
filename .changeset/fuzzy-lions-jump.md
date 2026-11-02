---
type: fix
"TursoSync": patch
---

Prevent local statement I/O from servicing a concurrent push or pull HTTP request, avoiding store operations that wait for the sync network round trip.