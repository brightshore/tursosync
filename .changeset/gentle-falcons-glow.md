---
type: feat
"TursoSync"
---

New `TursoConnection.WarmPool(connectionString, count)` leaves up to `count` idle connections in the pool (capped at `Max Idle Connections`). Call it before a pull you expect to be slow: a pull holds the sync engine for its whole download, so a new connection can't be opened until it ends, but a warmed one is rented from the pool and its queries run during the download. On a slow link this is the difference between an app that works during its startup pull and one that shows nothing until the pull finishes.