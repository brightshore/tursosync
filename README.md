# TursoSync

[![CI](https://github.com/tweeddev/tursosync/actions/workflows/ci.yml/badge.svg)](https://github.com/tweeddev/tursosync/actions/workflows/ci.yml)
[![NuGet](https://img.shields.io/nuget/v/TursoSync.svg)](https://www.nuget.org/packages/TursoSync)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

The **offline-sync + full-text-search layer for [Turso](https://github.com/tursodatabase/turso) on .NET.**
It adds **local↔cloud replication** (push/pull/checkpoint against the Turso sync engine) and **tantivy
full-text search** — the pieces the official [`Turso.Data.Sqlite`](https://www.nuget.org/packages/Turso.Data.Sqlite)
binding doesn't ship. It carries a familiar ADO.NET surface (modeled on `Turso.Data`) so Dapper, DbUp and the
`DbProviderFactory` pattern work, but in its **own `Turso.Sync` namespace** so it coexists with the official
package rather than colliding with it.

Reach for the **official `Turso.Data.Sqlite`** for base local/remote access, the SQLite-compat facade, EF Core,
and NativeAOT. Reach for **TursoSync** when you need offline replication or FTS — the two are complementary.

```csharp
using Turso.Sync;

// Local-only (offline fast path — plain engine, no sync overhead)
await using var conn = new TursoConnection("Data Source=app.db");
await conn.OpenAsync();

// …or synced against Turso Cloud
await using var synced = new TursoConnection(
    "Data Source=app.db;Remote Url=libsql://my-db.turso.io;Auth Token=…");
await synced.OpenAsync();
synced.SyncDatabase!.Push();   // push local changes
synced.SyncDatabase!.Pull();   // pull + apply remote changes
```

## Packages

| Package | What |
|---------|------|
| [`TursoSync`](https://www.nuget.org/packages/TursoSync) | The sync + FTS provider (namespace `Turso.Sync`): the sync engine (push/pull/checkpoint/stats), an ADO.NET surface (connection/command/reader/parameter/transaction, `TursoFactory`) to query synced databases, UDFs, aggregates, collations, load-extension, local at-rest encryption, connection pooling. |
| [`TursoSync.DbUp`](https://www.nuget.org/packages/TursoSync.DbUp) | DbUp database provider — `DeployChanges.To.TursoDatabase(connectionString)`. |
| [`TursoSync.Dapper`](https://www.nuget.org/packages/TursoSync.Dapper) | Dapper type handlers that round-trip `Ulid`, `DateTimeOffset` and `Guid` as portable `TEXT`. |

```sh
dotnet add package TursoSync
dotnet add package TursoSync.DbUp     # optional: migrations
dotnet add package TursoSync.Dapper   # optional: Dapper type handlers
```

## Highlights

- **Two lanes, one API.** No `Remote Url` → the plain local engine (`AsyncIO=0`, no IO pump); a remote (or
  `Sync=true`) → the sync engine. In **release** builds, local performance is on par with SQLite.
- **Familiar surface, own namespace.** Public types live in namespace `Turso.Sync` (`TursoConnection`,
  `TursoCommand`, …), modeled on `Turso.Data` so Dapper, DbUp and the `DbProviderFactory` pattern work — and
  namespaced so a project can reference both this and the official `Turso.Data.Sqlite` without clashing.
- **Connection pooling** on by default (`Pooling=false` to disable) — opening Turso is expensive, the pool
  makes the open-per-op pattern ~50× cheaper.
- **Extensibility:** `CreateFunction` / `CreateAggregate` / `CreateCollation` / `EnableExtensions` /
  `LoadExtension`.
- **Local at-rest encryption:** `…;Encryption Cipher=aes256gcm;Encryption Key=<hex>`.
- **Full-text search (tantivy):** `…;Experimental Index Method=true` enables `CREATE INDEX … USING fts`.
  Composes with encryption — the index lives inside the database file, so it's encrypted at rest too.

## Connection string keys

`Data Source` (required) · `Remote Url` · `Auth Token` · `Namespace` · `Bootstrap` · `Sync` · `Pooling`
· `Busy Timeout` · `Long Poll Timeout` · `Http Timeout` · `Push Operations Threshold` · `Encryption Cipher` · `Encryption Key`
· `Experimental Index Method`.

## Push batching

Pushes default to batches of 1,000 CDC operations, so a backlog accumulated offline can make progress
without uploading the entire change set in one request. Set `Push Operations Threshold` in the connection
string, or `TursoSyncConfig.PushOperationsThreshold`, to change the threshold. Zero explicitly restores
one request for the whole change set. A batch ends at a transaction boundary, so one larger transaction
can exceed the threshold. A retry resumes from the last batch the server acknowledged.

## Sync HTTP diagnostics

The opt-in HTTP diagnostics API requires **TursoSync 1.8.0 or later** (the next minor
release; 1.7.x does not contain it). This change does not publish a package. Consumers
such as Foxery must upgrade their TursoSync reference after that release is published.

Set `TursoSyncConfig.HttpDiagnostics` before `TursoSyncDatabase.Create` to include
bootstrap requests, or use `TursoSyncDatabase.HttpDiagnostics` on an open replica to
log a push:

```csharp
var database = synced.SyncDatabase!;
var previous = database.HttpDiagnostics;
try
{
    database.HttpDiagnostics = diagnostic => Console.WriteLine(diagnostic);
    database.Push();
}
finally
{
    database.HttpDiagnostics = previous;
}
```

The callback defaults to null (off). It receives one `TursoSyncHttpDiagnostic` when
an HTTP request finishes or fails: method, an allowlisted endpoint label (`info`,
`pull-updates`, `push-updates`, or `other`), request body bytes, time to response
headers, status, declared response bytes, actual bytes read, total duration, and
`FailurePhase` (`None`, `BeforeHeaders`, or `ResponseBody`). Unknown methods become
`OTHER`. No URL, query, arbitrary path, headers, credentials, payload, or exception
text is included. Null measurements mean unavailable; partial body bytes survive a
read failure. HTTP error statuses are reported unchanged and are not themselves
transport failures. Existing sync error handling and timeouts are unchanged.

Callbacks run synchronously under the sync operation lock; keep them fast and do
not call back into the database. Callback exceptions are swallowed. Timings exclude
the callback. Events arrive on completion, not while a request is stalled; an
infinite HTTP timeout can therefore postpone an event indefinitely. The callback
is shared by all connections using the same replica and captured per request:
coordinate temporary changes with other database users if logging only one push.

## Full-text search

Tantivy FTS is behind the engine's experimental `index_method` feature, so opt in with
`Experimental Index Method=true` (or `TursoSyncConfig.ExperimentalIndexMethod`). Without it,
`USING fts` fails to parse.

```csharp
await using var conn = new TursoConnection(
    "Data Source=app.db;Experimental Index Method=true");
await conn.OpenAsync();

conn.ExecuteNonQuery("CREATE INDEX idx_articles ON articles USING fts (title, body)");

// BM25-ranked search, with the matched terms highlighted.
using var cmd = conn.CreateCommand();
cmd.CommandText = """
    SELECT id, fts_highlight(body, '<mark>', '</mark>', @q)
    FROM articles
    WHERE fts_match(title, body, @q)
    ORDER BY fts_score(title, body, @q) DESC
    """;
```

Tokenizers (`default`/`raw`/`simple`/`whitespace`/`ngram`) and per-column weights are set with
`WITH (tokenizer = 'ngram', weights = 'title=2.0,body=1.0')`. Automatic segment merging is disabled —
run `OPTIMIZE INDEX <name>` on a cadence (like `VACUUM`/`ANALYZE`) after bulk or continuous inserts.

## Native library

The provider P/Invokes the `turso_sync_sdk_kit` native (the Turso sync engine + tantivy FTS). Released
packages carry it under `runtimes/<rid>/native/`. For local development against a self-built engine, point
`TURSOSYNC_NATIVE_DIR` at the folder containing the built `turso_sync_sdk_kit` library. **Build the native in
release** — debug builds are ~25× slower.

## Engine version

The Turso engine (the `turso_sync_sdk_kit` native) is **not vendored** — CI builds it from
[tursodatabase/turso](https://github.com/tursodatabase/turso) at the commit pinned in
[`turso-engine.json`](turso-engine.json), currently the stable **v0.7.0** release. The pin keeps builds
reproducible and lets us validate each engine bump before adopting it.

- Bump it with `scripts/bump-turso.sh <tag|latest>` (resolves the tag → commit SHA).
- A weekly **Engine bump** workflow opens a PR when a newer release appears in the pinned series; CI builds
  + tests against it on the PR, so an ABI change is caught in review before merge.

## Examples

```csharp
// Migrations (TursoSync.DbUp)
using DbUp;
var result = DeployChanges.To.TursoDatabase("Data Source=app.db")
    .WithScriptsEmbeddedInAssembly(typeof(Program).Assembly)
    .Build()
    .PerformUpgrade();

// Dapper type handlers (TursoSync.Dapper)
Turso.TursoTypeHandlers.Register();

// A scalar UDF
conn.CreateFunction("times_two", 1, args => Convert.ToInt64(args[0]) * 2);
```

## Status

TursoSync is the **sync + FTS complement** to the official [`Turso.Data.Sqlite`](https://www.nuget.org/packages/Turso.Data.Sqlite)
binding, not a replacement for it. Base local/remote access, the SQLite-compat facade, EF Core and NativeAOT
are official's domain — use that package directly for those. TursoSync owns the offline sync engine
(push/pull/checkpoint) and tantivy FTS, and carries just enough of an ADO.NET surface (modeled on `Turso.Data`,
see [TURSO-PARITY.md](TURSO-PARITY.md)) to query a synced database and support Dapper/DbUp. One known gap is
sync-lane at-rest encryption (base-lane encryption is supported).

## Testing

`rig test` (or `dotnet test`) runs the unit suite. The **live sync** suites are gated on environment
variables and report `Inconclusive` (skip) when unset:

- `LiveSyncIntegrationTests` — needs `TURSOSYNC_TEST_SYNC_SERVER` pointing at a `tursodb` binary; the harness
  starts a `tursodb --sync-server` on a free port per test.
- `TursoSyncBehaviorTests` — needs `TURSOSYNC_TEST_REMOTE_URL` (+ `TURSOSYNC_TEST_REMOTE_TOKEN`) for a real Turso
  Cloud round-trip.

Copy [.env.example](.env.example) to `.env` at the repo root and fill in what you have — the test project
loads it automatically. Real environment variables (and CI, which exports these) always take precedence.

## Releasing

Releases are driven by [shipRig](https://rigsmith.dev) (changesets) — config in `.changeset/`.

1. Record intent: `shiprig add` (pick the bump + summary; the three packages are `fixed`, so they
   version together; `TursoSync.Tests` is ignored).
2. Ship: `shiprig release` → versions + changelog → commit → tags `TursoSync@x.y.z` → push.

The pushed tag triggers the **Release** workflow, which builds all six RID natives (release + FTS,
stripped), packs, and publishes to NuGet via **trusted publishing** (OIDC — no API key). shipRig only
versions/tags/pushes; the cross-arch native build + publish stay in CI (`.changeset/release.jsonc`).

**Never run `shiprig publish` here.** It isn't in the pipeline, and `.changeset/config.json` names no package
source, so a local publish would pack on your machine, without the six native builds CI makes, and push that to
nuget.org. The only reason it doesn't today is that the version on disk is already published, so the push is
skipped as a duplicate. After a local `shiprig version` it would ship a broken package.

Day-to-day dev uses [rig](https://rigsmith.dev) (`.rig.json`): `rig build` / `rig test` / `rig coverage`,
plus `rig engine <tag|latest>` (pin the engine) and `rig pack` (local Tier-0 pack + consume test).

## License

MIT.
