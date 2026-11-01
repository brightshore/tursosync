---
type: fix
"TursoSync"
---

TursoSync now runs on Turso engine v0.8.3 (from v0.7.2). Turso 0.8 added two fields to the end of its database configuration struct, and without them every open failed with "unknown database open flags"; TursoSync now sends them, set to no page codec and default open flags. A replica opened by this version is stamped with engine 0.8.3, so treat the upgrade as one-way per replica: TursoSync 1.4.0 and later refuse to open it on an older engine (unless TURSOSYNC_IGNORE_STATE_GUARD=1 is set), but releases before 1.4.0 have no such check and would open it anyway, risking its sync state or a native crash. Don't open an upgraded replica with an older release.