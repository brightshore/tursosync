using System.Collections.Concurrent;
using Turso.Sync;

namespace TursoSync.Tests;

/// <summary>
/// Opening a connection to a synced database must ride out lock contention, not fail on it.
///
/// <para>The engine's <c>connect</c> runs a CDC pragma (and, the first time, a CDC-version read) on a brand-new
/// connection, through <c>prepare</c> — which never consults the busy handler — and before the caller has had
/// a chance to set a busy timeout at all. So a connect that lands while a checkpoint holds the lock failed
/// outright with <c>sync_database_connect resume: … database tape error: database is busy</c>, and a write
/// that opened its own connection was simply lost, on a store whose other writes were fine a moment
/// later.</para>
/// </summary>
[TestClass]
public class TursoConnectBusyTests
{
    private const int MeasureMs = 1500;
    private const int CheckpointEveryMs = 20;

    [TestMethod]
    public void Connect_UnderPeriodicCheckpoint_WaitsOutTheLockInsteadOfFailing()
    {
        if (!TursoNativeLibrary.IsAvailable())
        {
            Assert.Inconclusive("turso_sync_sdk_kit native library not found");
        }

        var dir = Directory.CreateTempSubdirectory("tursosync-connect-busy-");
        try
        {
            using var db = TursoSyncDatabase.Create(new TursoSyncConfig
            {
                Path = Path.Combine(dir.FullName, "store.db"),
                BootstrapIfEmpty = false,
            });
            using (var seed = TursoRawConnection.Open(db))
            {
                seed.Execute("CREATE TABLE t (x INTEGER PRIMARY KEY, payload TEXT)");
            }

            using var writerConn = TursoRawConnection.Open(db);
            using var done = new CancellationTokenSource(MeasureMs);
            var ct = done.Token;
            var payload = new string('p', 2048);

            // A writer keeps the WAL growing so every checkpoint has real work — that is what holds the lock.
            var writer = Task.Run(() =>
            {
                var n = 0;
                while (!ct.IsCancellationRequested)
                {
                    try { writerConn.Execute($"INSERT INTO t VALUES ({n++}, '{payload}')"); }
                    catch { /* contention is not what this test measures */ }
                }
            });
            var checkpointer = Task.Run(() =>
            {
                while (!ct.IsCancellationRequested)
                {
                    try { db.Checkpoint(); }
                    catch { /* likewise */ }
                    Thread.Sleep(CheckpointEveryMs);
                }
            });

            var opened = 0;
            var failures = new ConcurrentBag<string>();
            var opener = Task.Run(() =>
            {
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        using var conn = TursoRawConnection.Open(db);
                        Interlocked.Increment(ref opened);
                    }
                    catch (TursoException ex)
                    {
                        failures.Add(ex.Message.Split('\n')[0]);
                    }
                }
            });

            Task.WaitAll(writer, checkpointer, opener);

            opened.Should().BeGreaterThan(0, "the loop has to have opened something for the absence of failures to mean anything");
            failures.Should().BeEmpty("a contended connect must wait for the lock, as a contended step does");
        }
        finally
        {
            try { dir.Delete(recursive: true); } catch { /* temp */ }
        }
    }
}
