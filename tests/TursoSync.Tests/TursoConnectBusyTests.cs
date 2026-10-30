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

    /// <summary>A bound on waiting for a signal, so a regression fails instead of hanging. Never a verdict.</summary>
    private static readonly TimeSpan StuckAfter = TimeSpan.FromSeconds(20);

    /// <summary>
    /// A connect that waited out a push or pull for the database's gate gets its whole busy timeout once it has the
    /// gate. When the deadline counted from before the wait, a connect queued behind a long push had spent it all,
    /// and its first lock collision failed at once with <c>database is busy</c>. Runs on a fake clock: the gate is
    /// held while the clock jumps far past the timeout, then each attempt inside the gate is made to collide a few
    /// times.
    /// </summary>
    [TestMethod]
    public async Task Connect_That_Waited_For_The_Gate_Still_Gets_Its_Whole_Busy_Timeout()
    {
        using var fx = LocalSync.Create();
        var db = fx.Db;
        var clock = new FakeClock();
        db.Clock = clock.Read;
        db.Sleep = clock.Advance;
        var collisions = 3;
        db.ConnectAttemptStarting = () =>
        {
            if (collisions-- > 0)
            {
                throw new TursoException("simulated: database is busy", TursoStatus.Busy);
            }
        };

        using var held = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        var holder = new Thread(() =>
        {
            lock (db.Gate)
            {
                held.Set();
                release.Wait();
            }
        }) { IsBackground = true };
        holder.Start();
        held.Wait();

        var connect = Task.Factory.StartNew(() => db.Connect(busyTimeoutMs: 100), TaskCreationOptions.LongRunning);
        // Its second clock read is the last before it blocks on the gate (the deadline, then the start of the wait).
        clock.WaitForReads(2, StuckAfter).Should().BeTrue("the connect has to be waiting for the gate");
        clock.Advance(10_000); // a push holds the gate for a hundred busy timeouts
        release.Set();
        holder.Join();

        var handle = await connect.WaitAsync(StuckAfter);
        try
        {
            handle.Should().NotBe(IntPtr.Zero);
            collisions.Should().Be(-1, "all three collisions were retried and the fourth attempt connected");
        }
        finally
        {
            TursoNative.ConnectionClose(handle, out var errorPtr);
            if (errorPtr != IntPtr.Zero)
            {
                TursoNative.FreeString(errorPtr);
            }

            TursoNative.ConnectionDeinit(handle);
        }
    }

    /// <summary>The other half: not charging the gate wait must not make the timeout endless. With no gate wait, a
    /// connect that keeps colliding gives up once its busy timeout has gone by on the (fake) clock.</summary>
    [TestMethod]
    public void Connect_That_Keeps_Colliding_Gives_Up_When_Its_Busy_Timeout_Runs_Out()
    {
        using var fx = LocalSync.Create();
        var db = fx.Db;
        var clock = new FakeClock();
        db.Clock = clock.Read;
        db.Sleep = clock.Advance;
        var attempts = 0;
        db.ConnectAttemptStarting = () =>
        {
            attempts++;
            throw new TursoException("simulated: database is busy", TursoStatus.Busy);
        };

        var act = () => db.Connect(busyTimeoutMs: 100);

        act.Should().Throw<TursoException>().Which.IsBusy.Should().BeTrue();
        clock.Read().Should().BeGreaterThanOrEqualTo(100, "it backed off until the timeout had gone by");
        attempts.Should().BeGreaterThan(1, "a collision is retried while time remains");
    }

    private sealed class FakeClock
    {
        private long _now;
        private int _reads;

        public long Read()
        {
            Interlocked.Increment(ref _reads);
            return Volatile.Read(ref _now);
        }

        public void Advance(int ms) => Interlocked.Add(ref _now, ms);

        public bool WaitForReads(int count, TimeSpan bound) =>
            SpinWait.SpinUntil(() => Volatile.Read(ref _reads) >= count, bound);
    }

    private sealed class LocalSync : IDisposable
    {
        private readonly DirectoryInfo _dir;

        private LocalSync(DirectoryInfo dir, TursoSyncDatabase db)
        {
            _dir = dir;
            Db = db;
        }

        public TursoSyncDatabase Db { get; }

        public static LocalSync Create()
        {
            if (!TursoNativeLibrary.IsAvailable())
            {
                Assert.Inconclusive("turso_sync_sdk_kit native library not found");
            }

            var dir = Directory.CreateTempSubdirectory("tursosync-connect-clock-");
            return new LocalSync(dir, TursoSyncDatabase.Create(new TursoSyncConfig
            {
                Path = Path.Combine(dir.FullName, "store.db"),
                BootstrapIfEmpty = false,
            }));
        }

        public void Dispose()
        {
            Db.Dispose();
            try { _dir.Delete(recursive: true); } catch { /* temp */ }
        }
    }

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
