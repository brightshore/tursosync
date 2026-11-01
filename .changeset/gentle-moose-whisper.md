---
type: fix
"TursoSync"
---

The schema guard no longer breaks opening or pulling a replica that is already open. A second engine on a replica another engine in the process holds failed to open, and every pull in DetectAndBackup mode failed, because the guard copied files the open engine had locked (on macOS and Windows). When the replica is open, the guard now takes its backup as a consistent snapshot through that engine (VACUUM INTO), which is the database file alone, without the sync sidecars; a replica nothing holds is still backed up file by file.