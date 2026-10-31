# TursoSync

## 1.8.1
### 🩹 Fixes

- Split sync pushes into batches of 1,000 operations by default, preserving transactions and resuming from the last acknowledged batch after a failed request. Configure the threshold with Push Operations Threshold; zero retains an unlimited batch.

## 1.8.0

### 🚀 Enhancements

- Add opt-in credential-safe sync HTTP diagnostics callbacks with transport timings, byte counts and failure phases

## 1.7.0

### 🚀 Enhancements

- New `TursoConnection.TryOpenIdle()` opens the connection only when the pool holds an idle one, and returns `false` instead of connecting when it doesn't (or when `Pooling` is off). Use it where a caller must not wait on the network to open a connection. The guarantee covers opening only: closing the connection returns it to the pool as usual, which still runs the health probe and can wait. A push or pull holds the sync database for its whole round trip, and making a new connection waits for it to end, while renting an idle connection needs nothing from the engine. Checking `WarmPool(connectionString, 0)` before `Open` doesn't give that guarantee: another opener can take the last idle connection in between, and the open then connects. Unlike `Open`, it also skips the health probe a long-idle connection gets, since the probe is a statement and can itself wait on a push in flight; a connection that died while idle fails your first statement instead.

## 1.6.0

### 🚀 Enhancements

- A sync request now gives up when the server stops answering, after one minute by default, instead of after `HttpClient`'s 100 seconds. Set it with the new `Http Timeout` connection-string key (milliseconds; `TursoSyncConfig.HttpTimeoutMs`), or `-1` to wait forever. The timeout also applies to each read of a response body, so a download that stops arriving fails too, and it never cuts a configured `Long Poll Timeout` short. This matters beyond the failed request: a push or pull holds the sync database until it ends, and opening a new connection waits for it.
  
  Opening a connection no longer fails with "database is busy" just because it first had to wait for a push or pull to finish. The wait used to count against the connection's `Busy Timeout`, so after a long push the first lock collision failed at once. The busy timeout now covers lock contention only.

## 1.5.0

### 🚀 Enhancements

- New `TursoConnection.WarmPool(connectionString, count)` makes sure the pool holds at least `count` idle connections (capped at `Max Idle Connections`). Call it before a pull you expect to be slow, off the UI thread: a pull holds the sync engine for its whole download, so a new connection can't be opened until it ends, but a warmed one is rented from the pool and its queries run during the download. On a slow link this is the difference between an app that works during its startup pull and one that shows nothing until the pull finishes. A query that needs disk IO shares the engine's IO queue with the download and may still wait on part of it.

## 1.4.5

### 🩹 Fixes

- The Windows natives (`win-x64`, `win-arm64`) now link the MSVC C runtime statically, so they load on a machine without the Visual C++ redistributable installed. Before, such a machine failed to load `turso_sync_sdk_kit.dll` with "the specified module could not be found", which `TursoNativeLibrary.IsAvailable()` reported as no native library at all.

## 1.4.4

### 🩹 Fixes

- `ReconcileLocalTables` now reads the server's table list with one SQL request over `/v2/pipeline`, instead of bootstrapping a throwaway replica of the whole database to read a few names. On a large database that took about a minute and downloaded the full database on every call; it is now one round trip. If the server doesn't answer that request, it falls back to the bootstrap as before. When it does fall back, `TursoReconcileResult.PipelineFallbackReason` says why (null when the pipeline answered), so a wrong token or a server that doesn't speak Hrana can be logged instead of silently paying for the slow path.

## 1.4.3

### 🩹 Fixes

- The packages' project and repository links now point at github.com/brightshore/tursosync, where the code lives, instead of the old tweeddev/tursosync.
- A statement that finds the database locked while it is being prepared now waits for the lock instead of failing at once, as long as the connection is not inside a transaction. The engine's busy timeout only ever covered running a statement, not preparing it, so a statement that arrived during a checkpoint failed with "prepare_single: database is locked" in microseconds, whatever busy timeout was set. It now retries until the connection's busy timeout runs out, as opening a connection already did. Inside a transaction a busy prepare still fails straight away, because the transaction's snapshot may be stale and only rolling it back and starting again can help.

## 1.4.2

### 🩹 Fixes

- TursoConnection.OpenAsync no longer blocks its caller while a new connection is being opened. Opening a fresh connection on a synced database waits for any push or pull in progress, which can take as long as the network does; that wait now happens on the thread pool, so an app awaiting OpenAsync on its UI thread stays responsive. Renting an idle pooled connection is unchanged and still completes immediately.
- Opening several sync engines on the same database at once no longer fails intermittently with "Could not find file '…-info'". Each engine rewrote the database's -info sidecar through the same temporary file name, so concurrent writers could collide; every write now uses its own temporary file and still replaces the sidecar atomically. On Windows, rewriting or reading a sidecar while another engine is replacing it no longer fails with "Access to the path is denied" or "being used by another process", and a reader no longer sees the sidecar as momentarily missing (which the engine would have taken as no sync metadata).

## 1.4.1

### 🩹 Fixes

- Opening a connection to a synced database no longer fails with `database tape error: database is busy` when it lands during a checkpoint or another writer's lock. The open now waits up to the connection's busy timeout (5 seconds by default), the same as a statement already did, so a write that opens its own connection is no longer lost to momentary contention.

## 1.4.0
### Minor Changes

- Pre-open sync-state guard: foreign or unusable on-disk sync state (unparseable -info, unsupported metadata version, or state stamped by a newer engine) now throws a typed TursoSyncStateException before the native engine touches the files, instead of risking a native panic that aborts the host process. Successful opens stamp the engine version in a new -tursosync sidecar. TursoException is now unsealed so the new exception subclasses it. Escape hatch: TURSOSYNC_IGNORE_STATE_GUARD=1.

## 1.3.0
### Minor Changes

- Schema guard + ReconcileLocalTables: sync operations that drop local tables now throw TursoSchemaGuardException (with optional pre-operation backup) instead of losing them silently, and ReconcileLocalTables() repairs tables stranded by a pre-attach create — their schema is created on the server over Hrana `/v2/pipeline` (CDC never replicates DDL) and their rows re-recorded through the synced connection so the next push replays them
  

## 1.2.0
### Minor Changes

- Add an `ExperimentalIndexMethod` option that enables the engine's `index_method` feature, unlocking tantivy full-text search (`CREATE INDEX … USING fts`, `fts_match`/`fts_score`/`fts_highlight`, `OPTIMIZE INDEX`). Set it via the `Experimental Index Method` connection-string key or `TursoSyncConfig.ExperimentalIndexMethod`. Experimental features are now sent to the native as a comma-separated list, so encryption and FTS compose — an FTS index inside an encrypted database is itself encrypted at rest.

## 1.1.0
### Minor Changes

- Surface lock contention as `TursoException.IsBusy` so callers can tell a retryable busy from a real error.
  
  `Check` threw on the engine's error pointer *before* looking at the status code, so a `Busy` result was
  reported as a plain message with the status discarded — leaving string-matching on "database is locked" as
  the only way to classify it. The status is now carried on the exception in both branches.
  
  `IsBusy` and `IsBusySnapshot` are deliberately mutually exclusive. The engine treats them as different
  failures: plain `Busy` is lock contention worth retrying, while `BusySnapshot` means a checkpoint advanced the
  WAL and the read snapshot is permanently stale — "Retrying with busy_timeout will NEVER HELP", the transaction
  has to be rolled back and restarted. Both are also matched on message text, because contention sometimes
  arrives with a generic status and an explanatory string rather than the dedicated code.
  
  This does not retry anything. A concurrent checkpoint currently fails statements outright rather than making
  them wait: measured with one writer and a checkpoint every 100 ms, a contended `prepare_single` fails in ~3
  microseconds (median) whether the connection's busy timeout is 200 ms or 5000 ms — 25x the budget, identical
  time-to-failure — so the engine's busy handler is not consulted on that path. Without a checkpoint running,
  a concurrent writer produces no contention failures at all. `TursoBusyTimeoutTests` pins this so that a fix
  in the engine flips the assertion.
  
  A managed retry loop was tried and reverted: waiting out the busy means holding native statement state across
  a live checkpoint, and the workload stops draining entirely (a 3-second probe arm ran indefinitely at 165%
  CPU, versus 3.0 s with retry off). The rationale is recorded in `TursoRawConnection` so it is not
  reintroduced. The real fix belongs in the engine, next to the lock it needs to wait on.
  
- Typed error kinds, a normalized pool key, a configurable idle cap, and leak visibility.
  
  `TursoException.Kind` (`TursoErrorKind`) exposes the engine's own classification — `Constraint`, `ReadOnly`,
  `DatabaseFull`, `Corrupt`, `NotADatabase`, `Io`, `Interrupted`, `Busy`, `BusySnapshot` — so a caller can tell a
  unique-key violation from a real fault without matching on message text. `IsBusy`/`IsBusySnapshot` are now
  derived from it.
  
  Physical connections are pooled under a key built from the PARSED connection string rather than its raw text.
  The base builder normalizes keywords but not values, so `Sync=true` and `SYNC=True` previously keyed two
  separate pools for one connection, each keeping its own idle set; paths are resolved to full paths for the
  same reason. Only settings that make two connections non-interchangeable are part of the key.
  
  `Max Idle Connections` (aliases: `MaxIdleConnections`, `Max Pool Size`, `MaxPoolSize`) makes the per-key idle
  cap configurable, defaulting to the previous hard-coded 4.
  
  Renting no longer re-runs the health probe on a connection returned within the last second — it was proven
  healthy on the way in, and the probe is a full round trip on every checkout for consumers that open a
  connection per operation.
  
  `TursoConnection.OpenReplicas` lists the replicas currently held open and how many connections hold each. One
  sync engine is shared per replica and lives until its last connection is disposed, so a connection that is
  never closed pins its replica for the life of the process and no amount of `ClearPool()` will release its
  files; asserting this list is empty at shutdown catches that.
  

### Patch Changes

- Fix a connection-pool leak, and stop treating lock contention as a dead connection.
  
  `Rent` dequeued an idle connection and, if the health probe rejected it, returned it to nobody — the
  connection was dropped without being disposed. That always leaked the native connection; with connections now
  sharing a sync engine per replica it also leaked the engine reference, so the refcount never reached zero, the
  replica's files stayed open for the life of the process, and `ClearPool()` could no longer release them for
  `RemoteAttach` to move. `Rent` now disposes rejected connections and keeps draining the queue instead of
  falling straight through to opening a new one.
  
  `IsUsable` treated every exception as a dead connection, including `database is locked`. A checkpoint fails
  concurrent statements — the probe included — so the whole pool was discarded and rebuilt at exactly the moment
  it was most contended, against an engine that was still busy. Contention (`IsBusy`/`IsBusySnapshot`) now
  counts as healthy; only real failures retire a connection.
  
- Share one sync engine per replica file set instead of creating one per connection.
  
  `TursoPhysicalConnection.Create` called `TursoSyncDatabase.Create` for every physical connection, so a pool
  holding up to `MaxIdlePerKey` connections ran that many independent sync engines over a single replica — each
  owning and fragmenting the same WAL. Connections now acquire a reference-counted, shared engine
  (`TursoSyncDatabaseCache`) and open over it via `TursoSyncDatabase.Connect`, which is what that API is for.
  The engine is disposed when its last connection goes, so pooled connections keep it warm and `ClearPool()`
  still releases the replica's file handles.
  
  Measured against a live remote on a 13.8 MB WAL under 25 s of 4-way concurrent load with a checkpoint/push
  loop underneath. Engine per connection: 10,626 statements, 745,096 errors, 86 failed checkpoints, and
  `sync_database_create ... database tape error: database is busy` — connections that could not be opened at
  all while another engine was active. Shared engine, same load through the ADO.NET surface: 10,260 statements,
  125 errors, zero failed checkpoints, and no open-time failures whatsoever.
  
  Opening a replica against a different remote than the one it is already open against is now rejected with a
  clear error rather than silently handed an engine bound elsewhere.
  

## 1.0.1

## 1.0.0
### Minor Changes

- Add TursoSyncConfig.PullBytesThreshold to chunk the initial bootstrap download into multiple /pull-updates requests (0 = single round-trip, the default). Useful for large remote databases.

### Patch Changes

- Fix remote sync against Turso Cloud: zero-length request bodies (e.g. the initial /pull-updates protobuf, which encodes to no bytes) are now sent with their content-type and Content-Length: 0, so Cloud no longer rejects the bootstrap with HTTP 400. Bundled Turso engine bumped to v0.7.0.

### Breaking

- BREAKING: move public types from namespace Turso to Turso.Sync so the package coexists with the official Turso.Data.Sqlite (which owns namespace Turso). Update 'using Turso;' to 'using Turso.Sync;'. Also rename the live-test env vars TWEED_TURSO_SYNC_URL/TOKEN to TURSOSYNC_SYNC_URL/TOKEN.

## 0.1.0
### 🩹 Fixes

- Fix enumerating a TursoDataReader (foreach / DbEnumerator): GetDataTypeName no longer throws InvalidEnumArgumentException on the Unknown value kind (which occurs while building schema before the first row); it now falls back to the declared column type.
- Reject local at-rest encryption on the sync engine: TursoSyncDatabase.Create now throws NotSupportedException when an EncryptionCipher is set, instead of silently producing a synced database that cannot be reopened ("Decryption failed for page=1"). Local at-rest encryption remains supported on the base (local-only) lane via OpenLocal. Turso Cloud server-side encryption (remote key) is unaffected.
