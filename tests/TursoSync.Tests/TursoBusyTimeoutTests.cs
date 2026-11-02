using System.Collections.Concurrent;
using System.Diagnostics;
using Turso.Sync;

namespace TursoSync.Tests;

/// <summary>
/// Whether the connection's busy timeout is actually honoured, and where.
///
/// <para>Under concurrent load the dominant error by a wide margin used to be
/// <c>prepare_single: database is locked</c> — 744,934 of 745,096 errors in one 25 s run — even though every
/// connection is opened with a 5000 ms busy timeout, which should make a contended statement WAIT rather than
/// fail. The measurement that settles it is time-to-failure: a honoured timeout means a busy call blocks for
/// roughly that long before giving up, so a failure returning in microseconds means nothing waited. The
/// engine's busy handler covers step only; <see cref="TursoRawConnection.Prepare"/> now gives a prepare the
/// same wait outside a transaction, and these tests pin both halves of that contract.</para>
///
/// <para>A checkpoint no longer causes those busies: statements wait for it on
/// <see cref="TursoSyncDatabase.ApplyGate"/>. The tests of Prepare's own rules therefore run their checkpoints with
/// <see cref="TursoSyncDatabase.CheckpointTakesApplyGate"/> off, which is how they still meet a real engine busy.</para>
/// </summary>
[TestClass]
public class TursoBusyTimeoutTests
{
    private const int Writers = 1;
    private const int MeasureSeconds = 3;

    /// <summary>Gap between checkpoints in the contended arms. Frequent enough to collide with statements
    /// constantly, but leaving windows in which a waiting prepare can actually get through.</summary>
    private const int CheckpointEveryMs = 100;

    /// <summary>The two busy timeouts compared. Far enough apart (25x) that a honoured timeout could not
    /// produce the same time-to-failure for both.</summary>
    private const int ShortTimeoutMs = 200;
    private const int LongTimeoutMs = 5000;

    [TestMethod]
    public void Outside_A_Transaction_A_Prepare_Waits_Out_A_Checkpoint()
    {
        SkipUnlessNative();

        var noCheckpoint = Measure(ShortTimeoutMs, CheckpointEveryMs, checkpointing: false);
        var shortTimeout = Measure(ShortTimeoutMs, CheckpointEveryMs, checkpointing: true, gatedCheckpoint: false);
        var longTimeout = Measure(LongTimeoutMs, CheckpointEveryMs, checkpointing: true, gatedCheckpoint: false);

        Report($"{ShortTimeoutMs}ms, no checkpoint", noCheckpoint);
        Report($"{ShortTimeoutMs}ms + checkpoint", shortTimeout);
        Report($"{LongTimeoutMs}ms + checkpoint", longTimeout);

        // A concurrent writer alone must never fail a reader — WAL readers do not block on a writer.
        noCheckpoint.Busy.Should().Be(0, "a concurrent writer alone must never fail a reader");

        // A checkpoint every 100 ms used to fail statements at prepare in microseconds, tens of thousands of
        // times a run, whatever the timeout. A prepare now waits a checkpoint out. Within 5 s every checkpoint
        // ends, so nothing fails. Within 200 ms most do, but not all: on a Windows runner a checkpoint has
        // outlasted 200 ms (2 prepares failed there). Giving up then is right, so what the short arm checks is
        // that a prepare that fails waited the timeout out first, never that it failed fast.
        longTimeout.PrepareBusy.Should().Be(0, "a prepare outside a transaction waits out a checkpoint that ends");
        if (shortTimeout.PrepareBusy > 0)
        {
            shortTimeout.MinPrepareFailMs.Should().BeGreaterThanOrEqualTo(ShortTimeoutMs * 0.9,
                "a prepare that gives up has waited the busy timeout out, not failed at once");
        }
        shortTimeout.Ok.Should().BeGreaterThan(0);
        longTimeout.Ok.Should().BeGreaterThan(0);
    }

    [TestMethod]
    public void A_Prepare_Waits_No_Longer_Than_The_Busy_Timeout()
    {
        SkipUnlessNative();

        // Checkpoints back to back hold the lock nearly all the time, so a prepare often runs out its timeout.
        // When it does, it must give up at the timeout, not wait on past it, and not fail early either.
        var held = Measure(ShortTimeoutMs, checkpointEveryMs: 0, checkpointing: true, gatedCheckpoint: false);
        Report($"{ShortTimeoutMs}ms + constant checkpoints", held);

        // No prepare ran out its timeout, so there is nothing to time: say so rather than pass.
        if (held.PrepareBusy == 0)
        {
            Assert.Inconclusive("no prepare ran out its busy timeout under constant checkpoints, so the bound went unmeasured");
        }

        held.MedianPrepareFailMs.Should().BeGreaterThanOrEqualTo(ShortTimeoutMs * 0.9,
            "a prepare outside a transaction waits the busy timeout out before giving up");
        held.MaxPrepareFailMs.Should().BeLessThan(ShortTimeoutMs + 250,
            "the busy timeout bounds the whole prepare, retries included");
    }

    [TestMethod]
    public void Inside_A_Transaction_A_Busy_Prepare_Still_Fails_At_Once()
    {
        SkipUnlessNative();

        // A transaction holds a snapshot a checkpoint can make stale, and retrying on a stale snapshot spins
        // forever (the reverted step retry in TursoRawConnection's note). So inside one, a busy prepare must
        // surface straight away for the caller to roll back and restart.
        // Periodic, not constant: back-to-back checkpoints leave no gap for a transaction to open or even roll
        // back in, and the arm then measures a connection stuck in its transaction rather than prepares inside one.
        var inTx = Measure(LongTimeoutMs, CheckpointEveryMs, checkpointing: true, inTransaction: true, gatedCheckpoint: false);
        Report($"{LongTimeoutMs}ms + checkpoint, in a transaction", inTx);

        // Typically ~20 a run. A retry inside a transaction makes it 0, so any at all shows they weren't retried.
        // Zero is therefore the failure this test exists to catch, never an inconclusive run.
        inTx.PrepareBusy.Should().BeGreaterThan(0, "a busy prepare inside a transaction surfaces instead of being retried");
        inTx.MedianPrepareFailMs.Should().BeLessThan(LongTimeoutMs / 10.0,
            "inside a transaction a busy prepare is not retried");
    }

    [TestMethod]
    public void A_Checkpoint_Fails_No_Statement_In_Or_Out_Of_A_Transaction()
    {
        SkipUnlessNative();

        // The short timeout is the point: nothing here waits for the busy timeout, so it can't run out.
        var outside = Measure(ShortTimeoutMs, CheckpointEveryMs, checkpointing: true);
        var inside = Measure(ShortTimeoutMs, CheckpointEveryMs, checkpointing: true, inTransaction: true);
        Report($"{ShortTimeoutMs}ms + gated checkpoint", outside);
        Report($"{ShortTimeoutMs}ms + gated checkpoint, in a transaction", inside);

        outside.Busy.Should().Be(0, outside.TopMessage);
        inside.Busy.Should().Be(0, inside.TopMessage);
        outside.Ok.Should().BeGreaterThan(0);
        inside.Ok.Should().BeGreaterThan(0);
    }

    [TestMethod]
    public void A_Stale_Snapshot_Still_Fails_At_Once_After_A_Checkpoint()
    {
        SkipUnlessNative();

        // Waiting for a checkpoint must not turn into the spin in TursoRawConnection's note: a snapshot that has gone
        // stale fails the moment the transaction tries to write, with the error that says to restart it.
        var dbPath = TempDb();
        try
        {
            using var db = TursoSyncDatabase.Create(new TursoSyncConfig { Path = dbPath, BootstrapIfEmpty = false });
            using var conn = TursoRawConnection.Open(db, LongTimeoutMs);
            using var other = TursoRawConnection.Open(db, LongTimeoutMs);
            conn.Execute("CREATE TABLE t (x INTEGER PRIMARY KEY)");

            conn.Execute("BEGIN");
            conn.QueryScalar("SELECT count(*) FROM t");
            other.Execute("INSERT INTO t VALUES (1)");
            try { db.Checkpoint(); }
            catch (TursoException ex) when (ex.IsBusy) { /* the open read transaction may refuse it */ }

            var sw = Stopwatch.StartNew();
            var write = () => conn.Execute("INSERT INTO t VALUES (2)");
            write.Should().Throw<TursoException>().Which.IsBusySnapshot.Should().BeTrue();
            sw.ElapsedMilliseconds.Should().BeLessThan(LongTimeoutMs / 2, "a stale snapshot is not waited on");
            conn.Execute("ROLLBACK");
        }
        finally
        {
            Cleanup(dbPath);
        }
    }

    [TestMethod]
    public void A_User_Function_May_Checkpoint_The_Database_Running_It()
    {
        SkipUnlessNative();

        // The function runs inside a native step, which holds the gate shared. Checkpoint asks for it exclusively, and
        // ReaderWriterLockSlim refuses that upgrade unless the step's hold is let go first (ApplyGateHolds).
        var dbPath = TempDb();
        try
        {
            using var db = TursoSyncDatabase.Create(new TursoSyncConfig { Path = dbPath, BootstrapIfEmpty = false });
            using var conn = TursoRawConnection.Open(db, LongTimeoutMs);
            Exception? thrown = null;
            var ran = false;
            conn.RegisterScalarFunction("checkpoint_now", 0, deterministic: false, _ =>
            {
                ran = true;
                try { db.Checkpoint(); }
                catch (TursoException ex) when (ex.IsBusy) { /* the statement calling it may refuse it */ }
                catch (Exception ex) { thrown = ex; }
                return 1L;
            });

            conn.QueryScalar("SELECT checkpoint_now()").Should().Be(1L);

            ran.Should().BeTrue();
            thrown.Should().BeNull();
        }
        finally
        {
            Cleanup(dbPath);
        }
    }

    [TestMethod]
    public void A_Transaction_Holding_The_Write_Lock_Rides_Out_Checkpoints()
    {
        SkipUnlessNative();

        var dbPath = TempDb();
        try
        {
            using var db = TursoSyncDatabase.Create(new TursoSyncConfig { Path = dbPath, BootstrapIfEmpty = false });
            using var conn = TursoRawConnection.Open(db, LongTimeoutMs);
            using var writerConn = TursoRawConnection.Open(db, LongTimeoutMs);
            conn.Execute("CREATE TABLE t (x INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT)");

            var payload = new string('p', 2048);
            var committed = 0;
            var checkpointAttempts = 0;
            var failures = new ConcurrentQueue<string>();
            using var done = new CancellationTokenSource(TimeSpan.FromSeconds(MeasureSeconds));
            var ct = done.Token;

            var writer = Task.Run(() =>
            {
                while (!ct.IsCancellationRequested)
                {
                    try { writerConn.Execute($"INSERT INTO t (payload) VALUES ('{payload}')"); }
                    catch { /* this writer only keeps the WAL moving; the transactions below are measured */ }
                }
            });

            var checkpointer = Task.Run(() =>
            {
                while (!ct.IsCancellationRequested)
                {
                    // Most attempts fail, because a transaction holds the lock. They count anyway: the SDK fails
                    // statements for as long as an attempt runs, however it ends.
                    Interlocked.Increment(ref checkpointAttempts);
                    try { db.Checkpoint(); }
                    catch { /* that failure is not what is measured */ }
                    Thread.Sleep(CheckpointEveryMs);
                }
            });

            var transactions = Task.Run(() =>
            {
                while (!ct.IsCancellationRequested)
                {
                    var stage = "begin";
                    var open = false;
                    try
                    {
                        conn.Execute("BEGIN IMMEDIATE");
                        open = true;
                        stage = "read";
                        conn.QueryScalar("SELECT count(*) FROM t");
                        stage = "write";
                        conn.Execute($"INSERT INTO t (payload) VALUES ('{payload}')");
                        stage = "commit";
                        conn.Execute("COMMIT");
                        open = false;
                        Interlocked.Increment(ref committed);
                    }
                    catch (TursoException ex) when (ex.IsBusy || ex.IsBusySnapshot)
                    {
                        failures.Enqueue($"{stage}: {ex.Message.Split('\n')[0]}");
                        if (open)
                        {
                            RollBack(conn);
                        }
                    }
                }
            });

            Task.WaitAll(writer, checkpointer, transactions);

            var byKind = failures.GroupBy(f => f).Select(g => $"{g.Count()} x {g.Key}").ToArray();
            Console.WriteLine($"[busy] write-lock transactions: committed={committed} checkpoint-attempts={checkpointAttempts} " +
                $"failed={failures.Count} {string.Join("; ", byKind)}");

            committed.Should().BeGreaterThan(0);
            checkpointAttempts.Should().BeGreaterThan(1, "the transactions have to run alongside checkpoints");
            byKind.Should().BeEmpty("a checkpoint must not fail a statement in a transaction holding the write lock");
        }
        finally
        {
            Cleanup(dbPath);
        }
    }

    private sealed record Result(
        int Ok, int Busy, int PrepareBusy, double MinPrepareFailMs, double MaxPrepareFailMs, double MedianPrepareFailMs,
        string TopMessage);

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

    private static Result Measure(
        int busyTimeoutMs, int checkpointEveryMs, bool checkpointing, bool inTransaction = false, bool gatedCheckpoint = true)
    {
        Trace($"Measure(busyTimeout={busyTimeoutMs}, checkpointEvery={checkpointEveryMs}, checkpointing={checkpointing}, tx={inTransaction}) start");
        var dbPath = TempDb();
        try
        {
            using var db = TursoSyncDatabase.Create(new TursoSyncConfig { Path = dbPath, BootstrapIfEmpty = false });
            db.CheckpointTakesApplyGate = gatedCheckpoint;
            using (var seed = TursoRawConnection.Open(db, busyTimeoutMs))
            {
                seed.Execute("CREATE TABLE t (x INTEGER PRIMARY KEY, payload TEXT)");
            }

            var payload = new string('p', 2048);

            // Every connection is opened BEFORE the load starts. Opening is itself contended (Connect waits
            // that out — see TursoConnectBusyTests), and letting it race here would measure the wait instead of
            // the thing under test.
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

            // The measured connection: run a statement, and time the failures at prepare. In the transaction arm
            // each statement runs inside an open read transaction, restarted after any failure as a caller must.
            var ok = 0;
            var busy = 0;
            var prepareFailMs = new ConcurrentBag<double>();
            var messages = new ConcurrentBag<string>();
            var reader = Task.Run(() =>
            {
                var open = false;
                while (!ct.IsCancellationRequested)
                {
                    var sw = Stopwatch.StartNew();
                    try
                    {
                        if (inTransaction && !open)
                        {
                            readerConn.Execute("BEGIN");
                            open = true;
                            sw.Restart();
                        }

                        readerConn.QueryScalar("SELECT count(*) FROM t");
                        Interlocked.Increment(ref ok);
                    }
                    catch (Exception ex)
                    {
                        sw.Stop();
                        Interlocked.Increment(ref busy);
                        var message = ex.Message.Split('\n')[0];
                        messages.Add(message);
                        if (message.StartsWith("prepare_single", StringComparison.Ordinal) && (!inTransaction || open))
                        {
                            prepareFailMs.Add(sw.Elapsed.TotalMilliseconds);
                        }

                        if (open)
                        {
                            RollBack(readerConn);
                            open = false;
                        }
                    }
                }

                if (open)
                {
                    RollBack(readerConn);
                }
            });

            // Optionally, a checkpoint loop underneath — the job of whatever drives the sync engine. With a gap it
            // is periodic: real checkpoints are occasional, so the question is whether a statement can ride out
            // one that will actually end. With no gap it holds the lock nearly all the time, which is what the
            // deadline and transaction arms need.
            var checkpointer = checkpointing
                ? Task.Run(() =>
                {
                    while (!ct.IsCancellationRequested)
                    {
                        try { db.Checkpoint(); }
                        catch { /* contention is the point */ }
                        if (checkpointEveryMs > 0)
                        {
                            Thread.Sleep(checkpointEveryMs);
                        }
                    }
                })
                : Task.CompletedTask;

            Trace("  load running; waiting for drain");
            Task.WaitAll([reader, checkpointer, .. writers]);
            Trace($"  drained: ok={ok} busy={busy} prepareBusy={prepareFailMs.Count}");
            foreach (var c in writerConns)
            {
                c.Dispose();
            }

            var sorted = prepareFailMs.OrderBy(x => x).ToArray();
            return new Result(
                ok,
                busy,
                sorted.Length,
                sorted.Length > 0 ? sorted[0] : 0,
                sorted.Length > 0 ? sorted[^1] : 0,
                sorted.Length > 0 ? sorted[sorted.Length / 2] : 0,
                messages.GroupBy(m => m).OrderByDescending(g => g.Count()).FirstOrDefault()?.Key ?? "(none)");
        }
        finally
        {
            Cleanup(dbPath);
        }
    }

    /// <summary>
    /// End the reader's transaction for real. Inside a transaction a busy prepare isn't retried, and that
    /// includes ROLLBACK's own, so one attempt under constant checkpoints usually fails and leaves the
    /// connection in its transaction. The arm then measured a connection stuck there (BEGIN refused, a million
    /// times) instead of prepares inside a transaction.
    /// </summary>
    private static void RollBack(TursoRawConnection conn)
    {
        for (var attempt = 0; attempt < 1000; attempt++)
        {
            try
            {
                conn.Execute("ROLLBACK");
                return;
            }
            catch (TursoException ex) when (ex.IsBusy || ex.IsBusySnapshot)
            {
                Thread.Sleep(1);
            }
            catch (TursoException)
            {
                return; // no transaction left to roll back
            }
        }
    }

    private static void Report(string label, Result r) => Console.WriteLine(
        $"[busy] {label,-46} ok={r.Ok,-7} busy={r.Busy,-8} prepare-busy={r.PrepareBusy,-8} " +
        $"median-prepare-fail={r.MedianPrepareFailMs,8:F3}ms  max-prepare-fail={r.MaxPrepareFailMs,9:F3}ms  {r.TopMessage}");

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
