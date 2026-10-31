---
type: fix
"TursoSync"
---

Preserve pending local transaction boundaries during pull and replay, so later sync pushes continue draining offline backlogs in bounded batches while each pull remains atomic.