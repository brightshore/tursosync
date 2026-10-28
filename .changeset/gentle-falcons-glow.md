---
type: feat
"TursoSync"
---

New `TursoConnection.WarmPool(connectionString, count)` makes sure the pool holds at least `count` idle connections (capped at `Max Idle Connections`). Call it before a pull you expect to be slow, off the UI thread: a pull holds the sync engine for its whole download, so a new connection can't be opened until it ends, but a warmed one is rented from the pool and its queries run during the download. On a slow link this is the difference between an app that works during its startup pull and one that shows nothing until the pull finishes. A query that needs disk IO shares the engine's IO queue with the download and may still wait on part of it.
