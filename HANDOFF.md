# TursoSync — handoff

_Snapshot: 2026-06-17. Use this to resume work in a fresh chat._

## What this is

**TursoSync** is a **drop-in ADO.NET provider for [Turso](https://github.com/tursodatabase/turso)**
(namespace `Turso`) with **local↔cloud sync** + **tantivy FTS** — the pieces the official `Turso.Data`
binding has in source but never publishes. There is **no official Turso package on nuget.org** (their .NET
binding is `IsPackable=false`; the community ones are libsql-based), so TursoSync is first-of-kind: the real
Rust engine + sync + FTS with bundled natives. It was ported from `tursodatabase/turso`'s Go sync binding
(`bindings/go/driver_sync.go` → `TursoSyncDatabase`'s host-driven IO loop).

## This repo

- **github.com/brightshore/tursosync** (public; moved from tweeddev/tursosync). Local clone for `rig`/`shiprig`: this dir.
- **Downstream consumer:** the Tweed app (`github.com/JohnCampionJr/tweed`, private) uses these packages via
  NuGet from `Tweed.Store` — a real-world consumer + the original home (the code was extracted from there).

## Published

`TursoSync`, `TursoSync.DbUp`, `TursoSync.Dapper` — first real release **`0.1.0`** on nuget.org (multi-target
net8/9/10; `TursoSync` bundles all six RID natives under `runtimes/<rid>/native/`, ~40 MB). Cut 2026-06-18
via `shiprig release` (tags `TursoSync@0.1.0` etc. → `release.yml` built the 6 RID natives + published via
OIDC + created the GitHub Release). The earlier `0.1.0-preview.1` remains on the feed.

## Package family

- **TursoSync** — ADO.NET provider (`TursoConnection`/`Command`/`Reader`/`Parameter`/`Transaction`,
  `TursoFactory`), the **sync engine** (`TursoSyncDatabase`), UDFs/aggregates/collations/load-extension,
  local at-rest encryption, connection pooling. Namespace `Turso` (drop-in for official `Turso.Data`).
- **TursoSync.DbUp** — `DeployChanges.To.TursoDatabase(...)` (namespace `DbUp` / `DbUp.Turso`).
- **TursoSync.Dapper** — `TursoTypeHandlers.Register()` (Ulid/DateTimeOffset/Guid → TEXT).
- Parity map: `TURSO-PARITY.md` (feature-complete vs official `Turso.Data`+`Turso.Raw`).

## Key architecture decisions

- **Two lanes, one API:** no `Remote Url` → base engine (`AsyncIO=0`, no IO pump); remote (or `Sync=true`) →
  sync engine. In **release** builds local Turso ≈ SQLite; **debug natives are ~25× slower** (release is
  mandatory for shipped natives).
- **One cdylib, both layers:** `turso_sync_sdk_kit` exports base `turso_*` + `turso_sync_*` + tantivy FTS.
  Built `--features turso_sdk_kit/fts --release --config profile.release.strip=true` (strip keeps the package
  under nuget.org's **250 MB** cap — Linux debuginfo was ~120 MB → ~16 MB stripped).
- **Native resolution:** packaged natives resolve via `runtimes/<rid>/native` (NuGet deps.json). Dev override:
  env var **`TURSOSYNC_NATIVE_DIR`** → a folder holding the built native. `TursoNativeLibrary` also walks up
  for a `reference/turso-main/target/...` checkout as a dev convenience.
- **Engine pin:** `turso-engine.json` (tag + commit SHA) is the **source of truth**; CI/release read it.
  Currently `v0.7.0` → `e7cb62a8bd2f3655a661a621ee389365c1a1e43e`.

## CI / release (`.github/workflows/`)

- **`ci.yml`** — build+test on 3 OS; fetches the engine at the pinned SHA, builds the native (release+FTS),
  runs the suite.
- **`release.yml`** — triggered by shipRig's tag **`TursoSync@*`**; matrix builds all 6 RID natives on
  **native runners** (incl. `ubuntu-24.04-arm`, `windows-11-arm`; osx-x64 cross-built on Apple Silicon since
  `macos-13` Intel is being retired) → strip → pack → **NuGet trusted publishing (OIDC, keyless)** → creates
  a **GitHub Release** for the tag (auto-generated notes, prerelease-aware, attaches the .nupkgs).
- **`engine-bump.yml`** — weekly; opens a PR bumping `turso-engine.json` when the upstream series has a newer
  release; CI validates the new ABI on the PR before merge.
- **Repo config:** variable **`NUGET_USER=JohnCampionJr`**; a NuGet **trusted-publishing policy** (account →
  Trusted Publishing; owner/repo/workflow must match the run exactly). No `NUGET_API_KEY` needed.

## Tooling & release cycle

- **rig** (`.rig.json`) = dev loop: `rig build` / `rig test` / `rig coverage`, plus `rig engine <tag|latest>`
  (pin engine) and `rig pack` (Tier-0 local build+pack+consume test via `./local-pack.sh`).
  - Local test/coverage needs the native: set `TURSOSYNC_NATIVE_DIR`, or `TURSO_SRC` (a turso checkout) so
    `local-pack.sh`/`scripts/bump-turso.sh` can build it.
- **shiprig** (`.changeset/`) = releases. `config.json`: `fixed:[TursoSync,.DbUp,.Dapper]` (ship together),
  `ignore:[TursoSync.Tests]`. `release.jsonc` order = **version → commit → tag → push** (build+publish
  delegated to CI — the local machine can't cross-build the 6 natives, and we publish via OIDC). **Never run
  `shiprig publish` locally:** with no package source configured it would pack here, without the natives, and
  push to nuget.org.
- **Cut a release:** `shiprig add` (record change+bump) → `shiprig release` → tags `TursoSync@x.y.z` + pushes
  → `release.yml` builds+publishes. For previews: `shiprig pre enter preview` first (a plain patch/minor from
  `-preview.1` resolves to `0.1.0`, dropping the prerelease).

## Coverage

**~82% line / ~71% branch (107 tests).** Well-covered now incl. the ADO.NET surface — connection
lifecycle/pooling, transactions, command + reader, parameter collection, extension marshalling (all lifted
from the earlier 64.9%/35-test baseline). The **remote-sync path** (Push/Pull/Stats/Checkpoint + HTTP IO
handler) is exercised for real by `LiveSyncIntegrationTests` — they spawn a self-hosted `tursodb --sync-server`
(gated on **`TURSOSYNC_TEST_SYNC_SERVER`** → a `tursodb` binary; locally `cargo build -p turso_cli --release` then
export the path). Native-dependent tests skip in the fast lane and run in `integration.yml`. The external-cloud
round-trip (`PushPull_RoundTripsThroughRemote`, gated on `TURSOSYNC_TEST_REMOTE_URL`) remains for real Turso Cloud.
Remaining gaps: residual error/edge branches; trivial ctors in TursoException; TursoStats/EncryptionCipher names.

## CI lanes

- **`ci.yml` (fast, every push/PR):** C# restore/build/test only — no Rust. Native-gated tests skip. ~1–3 min.
- **`integration.yml` (gated):** builds the native (release+FTS) + `tursodb` and runs the full suite incl.
  live-sync, on **push to main / weekly / `TursoSync@*` tags / `full-ci`-labelled PRs**. Built binaries are
  cached by engine SHA. The **main run is what warms the cache** for PRs — GitHub scopes a cache to its
  creating ref and PRs read only their own ref or the default branch, so without a main run every PR rebuilds.
- **`release.yml`:** unchanged — `TursoSync@*` tag → 6 RID natives → OIDC publish → GitHub Release.

## Open items / next steps

1. ~~**Version decision:** cut `0.1.0` (drop `-preview`).~~ **Done — `0.1.0` shipped 2026-06-18.** Two of the
   original `1.0.0` holds are cleared — the **remote-sync path has coverage** (item 2) and **sync-lane at-rest
   encryption is resolved** (below) — so **`1.0.0`** now waits only on the Turso **engine reaching a stable
   (non-pre) release**.
   - **Sync-lane at-rest encryption (resolved):** *not supported upstream*, not merely untested. The Go
     binding this was ported from has no encryption fields on its sync config; the engine plumbs only a
     *remote* (Turso Cloud) encryption key, never the local pager cipher, and a synced DB created with a
     cipher writes a page it cannot reopen (`Decryption failed for page=1`). `TursoSyncDatabase.Create` now
     **throws `NotSupportedException`** when a cipher is set, instead of silently producing a lost-on-reopen
     DB. Local at-rest encryption stays a **base-engine** feature (local-only connection → `OpenLocal`;
     covered by `Encryption_RoundTrips_AndRejectsWrongKey`). Wiring the engine's *remote* key
     (`RemoteEncryptionKey`/`RemoteEncryptionCipher`, already on the native struct) for Cloud encrypted DBs is
     separate future work (needs a Cloud DB to validate).
2. ~~**Biggest test gap:** add a gated live-sync integration test.~~ **Done** — `LiveSyncIntegrationTests`
   spawns `tursodb --sync-server` and round-trips Push/Pull/Stats/Checkpoint via `integration.yml`.
3. **engine-bump PRs:** to get CI to run on the bot PR, add repo secret **`BUMP_PAT`** + enable "Allow GitHub
   Actions to create and approve pull requests." (Bump PRs carry the `full-ci` label so `integration.yml`
   validates the new ABI — but only once `BUMP_PAT` lets the PR trigger workflows at all.)
4. **Follow-ups from the coverage sweep:** the remaining ~18% is mostly hard-to-reach native error/IO-failure
   branches in `TursoSyncDatabase` (Poison/FullRead/FullWrite error paths) and trivial value-type ctors. Also
   wiring the sync engine's *remote* encryption key for Turso Cloud encrypted DBs (see item 1) is unstarted.

_Done: nuget API key rotated; GitHub Release step added to `release.yml`; Tweed swapped to the published
packages; live-sync integration tests added (item #2); sync-lane encryption guard (item #1 blocker); CI split
into fast + gated-integration lanes with SHA-keyed binary caching; coverage 65%→82%; **`0.1.0` released
(item #1)**._

## Sync-state guard (2026-08-09, unreleased — changeset pending)

The 2026-08-08 Tweed incident: an app bundling a different engine vintage rewrote a replica's `-info`
sidecar; every later launch of the 1.3.0-based app then died in a **Rust panic → `abort()`** inside
`turso_sync_operation_resume` during the first pull — a native abort no managed `catch` can reach, so the
app crash-looped until the replica was quarantined by hand.

Mitigation, in `TursoSyncStateGuard` (called from `TursoSyncDatabase.Create`, i.e. before the native engine
touches any file): `-info` must be parseable JSON with metadata version `v1`, and a new `-tursosync` stamp
sidecar records the native engine version (`turso_version()`) that last opened the DB — an older engine
refuses to open a newer engine's state (serde ignores unknown fields, which is exactly how foreign state
sneaks past the native parser). Violations throw `TursoSyncStateException` (subclass of the now-unsealed
`TursoException`). Escape hatch `TURSOSYNC_IGNORE_STATE_GUARD=1`. Tests: `TursoSyncStateGuardTests`.

**Open upstream gap:** the sdk-kit's `extern "C"` entry points don't `catch_unwind`, so any engine panic
still aborts the host process. Worth raising on `tursodatabase/turso` — panics should surface as
`TURSO_ERROR` like every other failure. The guard narrows the trigger surface; it can't close it.

## Gotchas learned

- nuget.org **250 MB** package cap → strip natives (done).
- **macos-13 Intel runners are fading** → osx-x64 is cross-built on Apple Silicon (reliable Mac cross-arch).
- ARM Linux/Windows runners are **free only on public repos** (this repo is public).
- MSTest 4 / Microsoft.Testing.Platform: a test csproj inheriting multi-`TargetFrameworks` reports nothing
  under `dotnet test` — pin the test project to a single `TargetFrameworks` (net10).
- Trusted publishing: the policy is **owner+repo+workflow, no package name**; the `user` must be the policy
  **creator's exact username**, and the repo/workflow fields must match the run's OIDC claims exactly (a
  repo-name typo was the only thing that broke the first release).
