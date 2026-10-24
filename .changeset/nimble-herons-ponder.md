---
type: fix
"TursoSync"
---

Opening several sync engines on the same database at once no longer fails intermittently with "Could not find file '…-info'". Each engine rewrote the database's -info sidecar through the same temporary file name, so concurrent writers could collide; every write now uses its own temporary file and still replaces the sidecar atomically.