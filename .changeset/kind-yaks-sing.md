---
type: fix
"TursoSync"
---

TursoConnection.OpenAsync no longer blocks its caller while a new connection is being opened. Opening a fresh connection on a synced database waits for any push or pull in progress, which can take as long as the network does; that wait now happens on the thread pool, so an app awaiting OpenAsync on its UI thread stays responsive. Renting an idle pooled connection is unchanged and still completes immediately.