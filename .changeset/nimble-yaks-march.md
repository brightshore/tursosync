---
type: fix
"TursoSync"
---

`ReconcileLocalTables` now reads the server's table list with one SQL request over `/v2/pipeline`, instead of bootstrapping a throwaway replica of the whole database to read a few names. On a large database that took about a minute and downloaded the full database on every call; it is now one round trip. If the server doesn't answer that request, it falls back to the bootstrap as before.