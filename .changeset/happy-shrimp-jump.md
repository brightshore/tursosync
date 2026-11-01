---
type: fix
"TursoSync"
---

Reads on a synced database no longer fail with "database is locked" while a pull is applying changes. The engine failed some of those statements at once instead of waiting out the busy timeout; statements now wait for the apply to finish, and a pull waits only for statements already running. The download before the apply holds nothing, so reads are not held up by a slow network.