using System.Data;
using System.Diagnostics;
using Turso.Sync;

namespace TursoSync.Tests;

/// <summary>
/// <see cref="TursoConnection.OpenAsync(CancellationToken)"/> must not block its caller on a cold open.
///
/// <para>On the sync lane a new connection goes through <see cref="TursoSyncDatabase.Connect"/>, which waits for
/// the database's gate, and a push or pull holds that gate for its whole HTTP round trip. OpenAsync used to call
/// <c>Open()</c> inline and return a completed task, so a UI thread awaiting it froze for as long as the network
/// took: seen as 4 and 8 second freezes in a desktop app, every one a UI thread in <c>ConnectOnce</c> waiting
/// on a push or pull.</para>
///
/// <para>Each test holds the gate on another thread, standing in for that push, so a cold open is genuinely
/// pending while the test looks at it.</para>
/// </summary>
[TestClass]
public class TursoOpenAsyncTests
{
    private const int GateHeldMs = 1500;

    [TestMethod]
    public async Task OpenAsync_ReturnsToTheCaller_WhileTheSyncGateIsHeld()
    {
        using var fx = GateFixture.Start();
        using var second = new TursoConnection(fx.ConnString);

        var call = Stopwatch.StartNew();
        var opening = second.OpenAsync();
        var returnedAfter = call.ElapsedMilliseconds;

        returnedAfter.Should().BeLessThan(GateHeldMs / 4,
            "OpenAsync must hand control back to its caller while the connect waits for the gate");
        opening.IsCompleted.Should().BeFalse("the gate is still held, so the open cannot have finished");
        second.State.Should().Be(ConnectionState.Connecting, "an open in flight is Connecting, per DbConnection.OpenAsync");

        await opening.WaitAsync(TimeSpan.FromSeconds(30));
        second.State.Should().Be(ConnectionState.Open, "once the gate is released the open completes");
    }

    [TestMethod]
    public async Task Close_WhileOpening_AbandonsTheOpen_AndReleasesWhatItMade()
    {
        string replica;
        using (var fx = GateFixture.Start())
        {
            replica = fx.DbPath;
            using var second = new TursoConnection(fx.ConnString);
            var opening = second.OpenAsync();
            second.Close();
            second.State.Should().Be(ConnectionState.Closed, "Close abandons the pending open");

            var act = async () => await opening.WaitAsync(TimeSpan.FromSeconds(30));
            await act.Should().ThrowAsync<OperationCanceledException>("the open was abandoned, not completed");
            second.State.Should().Be(ConnectionState.Closed, "the finished connect must not install itself after Close");
        }

        TursoConnection.OpenReplicas.Should().NotContain(r => r.Path == replica,
            "the connection the abandoned open made is released, so nothing pins the replica");
    }

    [TestMethod]
    public async Task Cancelling_WhileOpening_ThrowsAndReleasesWhatItMade()
    {
        string replica;
        using (var fx = GateFixture.Start())
        {
            replica = fx.DbPath;
            using var second = new TursoConnection(fx.ConnString);
            using var cts = new CancellationTokenSource();
            using var started = new ManualResetEventSlim();
            TursoConnection.ColdOpenStarting = started.Set;
            try
            {
                var opening = second.OpenAsync(cts.Token);
                // Cancel only once the connect is under way and waiting on the gate. Cancelling earlier stops it
                // before it starts, which is Task.Run's own check and not the path under test.
                started.Wait(TimeSpan.FromSeconds(10)).Should().BeTrue("the cold connect should have started");
                cts.Cancel();

                var act = async () => await opening.WaitAsync(TimeSpan.FromSeconds(30));
                await act.Should().ThrowAsync<OperationCanceledException>(
                    "a cancel that arrives during the connect is honoured when it returns, not ignored");
                second.State.Should().Be(ConnectionState.Closed);
            }
            finally
            {
                TursoConnection.ColdOpenStarting = null;
            }
        }

        TursoConnection.OpenReplicas.Should().NotContain(r => r.Path == replica);
    }

    [TestMethod]
    public async Task A_Second_Open_While_Opening_Is_Refused()
    {
        using var fx = GateFixture.Start();
        using var second = new TursoConnection(fx.ConnString);
        var opening = second.OpenAsync();

        second.Invoking(c => c.Open()).Should().Throw<InvalidOperationException>()
            .WithMessage("*already opening*", "two opens would make two physical connections and leak one");
        await second.Awaiting(c => c.OpenAsync()).Should().ThrowAsync<InvalidOperationException>();

        await opening.WaitAsync(TimeSpan.FromSeconds(30));
        second.State.Should().Be(ConnectionState.Open);
    }

    [TestMethod]
    public async Task OpenAsync_RentsAnIdlePooledConnection_WithoutAThreadHop()
    {
        if (!TursoNativeLibrary.IsAvailable())
        {
            Assert.Inconclusive("turso_sync_sdk_kit native library not found");
        }

        var dir = Directory.CreateTempSubdirectory("tursosync-open-async-pool-");
        try
        {
            var connString = $"Data Source={Path.Combine(dir.FullName, "store.db")}";
            using (var warm = new TursoConnection(connString))
            {
                warm.Open();   // returned to the pool on dispose
            }

            using var conn = new TursoConnection(connString);
            var opening = conn.OpenAsync();
            opening.IsCompleted.Should().BeTrue("an idle pooled connection is rented inline: the common case stays cheap");
            await opening;
            conn.State.Should().Be(ConnectionState.Open);
        }
        finally
        {
            TursoConnection.ClearPool();
            try { dir.Delete(recursive: true); } catch { /* temp */ }
        }
    }

    /// <summary>A local sync-lane database (no remote; <c>Pooling=false</c>, so every open is a real connect through
    /// the gate) whose gate is held on another thread for <see cref="GateHeldMs"/>, as a push on the network would.
    /// Dispose waits the hold out and removes the files.</summary>
    private sealed class GateFixture : IDisposable
    {
        private readonly DirectoryInfo _dir;
        private readonly TursoConnection _first;
        private readonly Thread _holder;

        private GateFixture(DirectoryInfo dir, TursoConnection first, Thread holder)
        {
            _dir = dir;
            _first = first;
            _holder = holder;
        }

        public string DbPath => Path.Combine(_dir.FullName, "store.db");
        public string ConnString => $"Data Source={DbPath};Sync=true;Bootstrap=false;Pooling=false";

        public static GateFixture Start()
        {
            if (!TursoNativeLibrary.IsAvailable())
            {
                Assert.Inconclusive("turso_sync_sdk_kit native library not found");
            }

            var dir = Directory.CreateTempSubdirectory("tursosync-open-async-");
            var connString = $"Data Source={Path.Combine(dir.FullName, "store.db")};Sync=true;Bootstrap=false;Pooling=false";
            var first = new TursoConnection(connString);
            first.Open();
            var db = first.SyncDatabase ?? throw new AssertFailedException("the sync lane should have a sync database");

            using var held = new ManualResetEventSlim();
            var holder = new Thread(() =>
            {
                lock (db.Gate)
                {
                    held.Set();
                    Thread.Sleep(GateHeldMs);
                }
            }) { IsBackground = true };
            holder.Start();
            held.Wait();
            return new GateFixture(dir, first, holder);
        }

        public void Dispose()
        {
            _holder.Join();
            _first.Dispose();
            try { _dir.Delete(recursive: true); } catch { /* temp */ }
        }
    }
}
