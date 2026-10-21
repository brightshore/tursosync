using System.Collections.Concurrent;
using System.Diagnostics;
using Turso.Sync;

namespace TursoSync.Tests;

/// <summary>
/// Whether the connection's busy timeout is actually honoured, and where.
///
/// <para>Under concurrent load the dominant error by a wide margin is
/// <c>prepare_single: database is locked</c> — 744,934 of 745,096 errors in one 25 s run — even though every
/// connection is opened with a 5000 ms busy timeout, which should make a contended statement WAIT rather than
/// fail. The measurement that settles it is time-to-failure: a honoured timeout means a busy call blocks for
/// roughly that long before giving up, so a failure returning in microseconds means the handler was never
/// consulted on that path.</para>
/// </summary>
[TestClass]
public class TursoBusyTimeoutTests
{
    private const int Writers = 1;
    private const int MeasureSeconds = 3;

    /// <summary>Gap between checkpoints in the contended arms. Frequent enough to collide with statements
    /// constantly, but leaving windows in which a retry can actually get through.</summary>
    private const int CheckpointEveryMs = 100;

    /// <summary>The two busy timeouts compared. Far enough apart (25x) that a honoured timeout could not
    /// produce the same time-to-failure for both.</summary>
    private const int ShortTimeoutMs = 200;
    private const int LongTimeoutMs = 5000;

    [TestMethod]
    public void BusyTimeout_TimeToFailure_AtPrepareAndStep()
    {
        SkipUnlessNative();

        var noCheckpoint = Measure(ShortTimeoutMs, checkpointing: false);
        var shortTimeout = Measure(ShortTimeoutMs, checkpointing: true);
        var longTimeout = Measure(LongTimeoutMs, checkpointing: true);

        Report($"{ShortTimeoutMs}ms, no checkpoint", noCheckpoint);
        Report($"{ShortTimeoutMs}ms + checkpoint", shortTimeout);
        Report($"{LongTimeoutMs}ms + checkpoint", longTimeout);

        // A concurrent writer alone must never fail a reader — WAL readers do not block on a writer.
        noCheckpoint.Busy.Should().Be(0, "a concurrent writer alone must never fail a reader");

        // A concurrent checkpoint, on the other hand, fails statements outright.
        shortTimeout.Busy.Should().BeGreaterThan(0, "a concurrent checkpoint is what produces contention");
        longTimeout.Busy.Should().BeGreaterThan(0, "a concurrent checkpoint is what produces contention");

        // THE DEFECT, pinned. A honoured busy timeout would make a contended statement wait — raising it 25x
        // would raise time-to-failure with it. Instead both fail in microseconds: the engine's busy handler
        // is not consulted on this path. When that is fixed in the engine, this assertion flips and should be
        // rewritten to require the wait rather than document its absence.
        longTimeout.MedianFailMs.Should().BeLessThan(LongTimeoutMs / 10.0,
            "the busy timeout is NOT consulted — statements fail immediately instead of waiting it out");
        longTimeout.MedianFailMs.Should().BeApproximately(shortTimeout.MedianFailMs, 5.0,
            "time-to-failure is independent of the configured timeout, which is the tell");
    }

    private sealed record Result(int Ok, int Busy, double MaxFailMs, double MedianFailMs, string TopMessage);

    /// <summary>Progress to a file, because MSTest buffers a test's stdout until the method returns — which
    /// is no use when the question is where a run is getting stuck.</summary>
    private static void Trace(string message)
    {
        var path = Environment.GetEnvironmentVariable("TURSOSYNC_PROBE_TRACE");
        if (!string.IsNullOrEmpty(path))
        {
            File.AppendAllText(path, $"{DateTime.Now:HH:mm:ss.fff} {message}\n");
        }
    }

    private static Result Measure(int busyTimeoutMs, bool checkpointing)
    {
        Trace($"Measure(busyTimeout={busyTimeoutMs}, checkpointing={checkpointing}) start");
        var dbPath = TempDb();
        try
        {
            using var db = TursoSyncDatabase.Create(new TursoSyncConfig { Path = dbPath, BootstrapIfEmpty = false });
            using (var seed = TursoRawConnection.Open(db, busyTimeoutMs))
            {
                seed.Execute("CREATE TABLE t (x INTEGER PRIMARY KEY, payload TEXT)");
            }

            var payload = new string('p', 2048);

            // Every connection is opened BEFORE the load starts. Opening is itself contended (Connect now
            // waits that out — see TursoConnectBusyTests), and letting it race here would measure the wait
            // instead of the thing under test.
            Trace("  seeded; opening connections");
            var writerConns = Enumerable.Range(0, Writers).Select(_ => TursoRawConnection.Open(db, busyTimeoutMs)).ToArray();
            using var readerConn = TursoRawConnection.Open(db, busyTimeoutMs);
            Trace("  connections open; starting load");

            using var done = new CancellationTokenSource(TimeSpan.FromSeconds(MeasureSeconds));
            var ct = done.Token;

            // Writers hold the write lock as continuously as they can.
            var writers = writerConns.Select((conn, w) => Task.Run(() =>
            {
                var n = 0;
                while (!ct.IsCancellationRequested)
                {
                    try { conn.Execute($"INSERT INTO t VALUES ({(w * 1_000_000) + n++}, '{payload}')"); }
                    catch { /* contention is the point */ }
                }
            })).ToArray();

            // The measured connection: run a statement, and time the failures.
            var ok = 0;
            var failMs = new ConcurrentBag<double>();
            var messages = new ConcurrentBag<string>();
            var reader = Task.Run(() =>
            {
                while (!ct.IsCancellationRequested)
                {
                    var sw = Stopwatch.StartNew();
                    try
                    {
                        readerConn.QueryScalar("SELECT count(*) FROM t");
                        Interlocked.Increment(ref ok);
                    }
                    catch (Exception ex)
                    {
                        sw.Stop();
                        failMs.Add(sw.Elapsed.TotalMilliseconds);
                        messages.Add(ex.Message.Split('\n')[0]);
                    }
                }
            });

            // Optionally, a checkpoint loop underneath — the SyncCoordinator's job. Periodic, NOT continuous:
            // a tight loop holds the lock permanently, under which no amount of retrying can ever succeed and
            // the only effect is to convert fast failures into slow ones. Real checkpoints are occasional
            // (SyncCoordinator's is every 20 s), so the question worth answering is whether a statement can
            // ride out a checkpoint that will actually end.
            var checkpointer = checkpointing
                ? Task.Run(() =>
                {
                    while (!ct.IsCancellationRequested)
                    {
                        try { db.Checkpoint(); }
                        catch { /* contention is the point */ }
                        Thread.Sleep(CheckpointEveryMs);
                    }
                })
                : Task.CompletedTask;

            Trace("  load running; waiting for drain");
            Task.WaitAll([reader, checkpointer, .. writers]);
            Trace($"  drained: ok={ok} busy={failMs.Count}");
            foreach (var c in writerConns)
            {
                c.Dispose();
            }

            var sorted = failMs.OrderBy(x => x).ToArray();
            return new Result(
                ok,
                sorted.Length,
                sorted.Length > 0 ? sorted[^1] : 0,
                sorted.Length > 0 ? sorted[sorted.Length / 2] : 0,
                messages.GroupBy(m => m).OrderByDescending(g => g.Count()).FirstOrDefault()?.Key ?? "(none)");
        }
        finally
        {
            Cleanup(dbPath);
        }
    }

    private static void Report(string label, Result r) => Console.WriteLine(
        $"[busy] {label,-22} ok={r.Ok,-7} busy={r.Busy,-8} median-fail={r.MedianFailMs,8:F3}ms  max-fail={r.MaxFailMs,9:F3}ms  {r.TopMessage}");

    private static void SkipUnlessNative()
    {
        if (!TursoNativeLibrary.IsAvailable())
        {
            Assert.Inconclusive("turso_sync_sdk_kit native library not found");
        }
    }

    private static string TempDb()
    {
        var dir = Path.Combine(Path.GetTempPath(), "tursosync-busy-" + Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(dir);
        return Path.Combine(dir, "store.db");
    }

    private static void Cleanup(string dbPath)
    {
        var dir = Path.GetDirectoryName(dbPath);
        if (dir is not null && Directory.Exists(dir))
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* temp */ }
        }
    }
}
