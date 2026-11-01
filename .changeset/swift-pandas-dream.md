---
type: fix
"TursoSync"
---

TursoSync now runs on Turso engine v0.8.3 (from v0.7.2). Turso 0.8 added two fields to the end of its database configuration struct, and without them every open failed with "unknown database open flags"; TursoSync now sends them, set to no page codec and default open flags. A replica opened by this version is stamped with engine 0.8.3, and older TursoSync releases will refuse to open it.