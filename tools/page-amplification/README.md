# Pull page amplification probe

This opt-in diagnostic measures how many pages `/pull-updates` returns compared
with how many actually differ from a retained baseline. It uses Python's standard
library and exercises the protocol directly, independently of TursoSync's HTTP
transport, native engine, CDC replay, and local WAL.

The fixture is 256 rows containing a one-byte integer marker and a random 4 KiB
payload. Only row 129's marker changes. Each update commits separately; repeated
pulls retain the original revision to simulate a client falling behind. Every
response is reconstructed into a private SQLite image and queried to check that
the committed marker arrived. Same-revision controls must return no pages. A
recent-revision control checks delivery of one more write after the old revision
has been exercised.

## Run

With an authenticated `turso` CLI, create a disposable 8 MiB-limited cloud database:

```sh
python3 tools/page-amplification/probe.py --cloud --group default \
  --output /tmp/cloud-pages.json
```

The tool accepts no existing database URL. It creates a uniquely named database,
keeps its one-day token in memory, and destroys the database in `finally`. The
report records its name and whether cleanup succeeded. If the process is forcibly
terminated, destroy that named database with `turso db destroy NAME --yes`.
Cloud usage can incur charges. Do not run Python with `-O`: assertions verify the
fixture, response format, and controls.

For a local control, supply a `tursodb` binary supporting `--sync-server`:

```sh
python3 tools/page-amplification/probe.py --local-binary /path/to/tursodb \
  --output /tmp/local-pages.json
```

The local server runs on loopback inside a temporary directory. The tool terminates
only that child process and removes its files when finished.

Use `--ages 0,5,30,120,300` to hold the same revision for five minutes before
generating more writes. `--commits 1,257,513,1101` chooses the total committed
updates since the baseline at each measurement. Defaults use odd counts so the
marker still differs from its baseline; even counts restore the original marker,
so returned pages can be byte-identical despite having been written.

Reports are saved atomically after each measurement and at exit. They contain
page numbers, counts, response sizes, elapsed time, SQLite version, and cleanup
status. They contain no tokens, connection strings, or row payloads. Retain reports
when comparing server versions; elapsed times include measurement requests and
are diagnostic observations, not isolated throughput benchmarks.

## Observations, 2026-11-01

The checked-in reports preserve the initial controlled experiments:

* Immediate cloud one-row updates returned one changed page. No-write and
  same-value-update controls returned zero pages.
* Retaining the same revision for 300 seconds still returned one changed page.
* After 1,100 additional one-row commits, that older revision returned **32 pages:
  one changed and 31 byte-identical to the baseline**, aligned on a 32-page boundary.
* Holding one committed change for 35 seconds and then adding two more commits
  still returned exactly one changed page. Elapsed time plus a few writes alone
  did not reproduce the transition.
* Repeated cloud runs switched to 32-page responses after a few hundred commits.
  One run was exact through 481 commits and coarse at 513; another was already
  coarse at 481. This is not evidence of a fixed commit-count threshold.
* After the older revision became coarse, the next write pulled from a recent
  revision still returned exactly one changed page.
* The local Turso 0.8.3 control returned four changed pages for a marker update
  and did not exhibit the cloud's aligned 32-page response through 1,100 commits.

These observations reproduce cloud page-selection amplification without a client
WAL or CDC replay. They support a history-dependent server fallback; they do not
identify the server's internal checkpoint or retention mechanism. An attempted
cloud `PRAGMA wal_checkpoint(TRUNCATE)` was rejected with `SQL_PARSE_ERROR`, so
the `after_checkpoint` measurement in the age report does **not** prove anything
about a checkpoint.

The cloud databases in the retained reports were destroyed. No production
database was written by these experiments.

Turso's [cloud storage architecture description](https://turso.tech/blog/how-does-the-turso-cloud-keep-your-data-durable-and-safe)
explains that checkpointed database generations consist of 128 KiB segments and
that checkpoint frequency depends on write volume and pattern. This is consistent
with the observed 32 × 4 KiB responses. Crossing a server checkpoint/generation
boundary is the leading explanation, but the probe cannot inspect server
checkpoint state or its page-selection implementation.

## Local WAL regression and fix

[PR #33](https://github.com/brightshore/tursosync/pull/33) adds the maintained native
patch for skipping unchanged incoming pages after local rollback. The retained
`wal-filter-results.json` records the real-database regression: the original code
wrote 74 frames for 74 unchanged pages; the patched code writes one commit frame.
A 75-page response with three changed pages writes three frames. The test checks
row values and `integrity_check`, overlapping local edits and rollback, database
growth, and the path with filtering disabled. This measures local WAL writes;
it does not change the cloud page-selection measurements above.

After checking out the pinned native engine and applying the maintained patches,
run its regression **from the native engine directory** so Cargo loads its compiler
configuration:

```sh
cargo test -p turso_sync_engine --lib unchanged_pulled_pages_do_not_refill_wal -- --nocapture
```

The source audit in `prerelease-page-apply-audit.json` compares the relevant
WAL-apply and page-append functions across 0.8.3, both 0.9.0 prereleases, and the
recorded upstream main commit. They are byte-identical. This is a source audit,
not a claim that prerelease binaries were tested.

## Live application observation after 1.9.2

`results/live-1.9.2-results.json` preserves counts from the application logs and
read-only replica inspection after upgrading to the published release. With a
lane generating new work, a push starting with 60,567 pending operations finished
in 44.7 seconds, and all 28 recorded HTTP requests returned 200. The first catch-up
pull transferred 85.3 MB for a 149.6 MB raw page stream. The next applied pull
transferred 362 KB in 1.9 seconds and grew the WAL by 2.7 MB. Successful push
timestamps continued advancing.

This is one live workload, not a controlled before/after benchmark. The WAL delta
includes rollback, remote application, and local replay. A stable copy of the
later operation's WAL showed 639 of 646 appended frames changed page contents;
seven repeated the preceding image. These are page images, not counts of business
changes. The cloud page-selection mechanism itself was not changed.
