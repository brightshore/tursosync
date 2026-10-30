---
type: feat
"TursoSync"
---

New `TursoConnection.TryOpenIdle()` opens the connection only when the pool holds an idle one, and returns `false` instead of connecting when it doesn't (or when `Pooling` is off). Use it where a caller must not wait on the network to open a connection. The guarantee covers opening only: closing the connection returns it to the pool as usual, which still runs the health probe and can wait. A push or pull holds the sync database for its whole round trip, and making a new connection waits for it to end, while renting an idle connection needs nothing from the engine. Checking `WarmPool(connectionString, 0)` before `Open` doesn't give that guarantee: another opener can take the last idle connection in between, and the open then connects. Unlike `Open`, it also skips the health probe a long-idle connection gets, since the probe is a statement and can itself wait on a push in flight; a connection that died while idle fails your first statement instead.
