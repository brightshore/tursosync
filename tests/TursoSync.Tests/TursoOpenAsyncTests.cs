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
/// </summary>
[TestClass]
public class TursoOpenAsyncTests
{
    private const int GateHeldMs = 2000;

    [TestMethod]
    public async Task OpenAsync_ReturnsToTheCaller_WhileTheSyncGateIsHeld()
    {
        if (!TursoNativeLibrary.IsAvailable())
        {
            Assert.Inconclusive("turso_sync_sdk_kit native library not found");
        }

        var dir = Directory.CreateTempSubdirectory("tursosync-open-async-");
        try
        {
            // Sync lane with no remote, and no pooling, so every open is a real connect through the gate.
            var connString = $"Data Source={Path.Combine(dir.FullName, "store.db")};Sync=true;Bootstrap=false;Pooling=false";
            using var first = new TursoConnection(connString);
            first.Open();
            var db = first.SyncDatabase ?? throw new AssertFailedException("the sync lane should have a sync database");

            // Stand in for a push that is on the network: hold the gate on another thread for a while.
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

            using var second = new TursoConnection(connString);
            var call = Stopwatch.StartNew();
            var opening = second.OpenAsync();
            var returnedAfter = call.ElapsedMilliseconds;

            returnedAfter.Should().BeLessThan(GateHeldMs / 4,
                "OpenAsync must hand control back to its caller while the connect waits for the gate");
            opening.IsCompleted.Should().BeFalse("the gate is still held, so the open cannot have finished");

            await opening.WaitAsync(TimeSpan.FromSeconds(30));
            second.State.Should().Be(ConnectionState.Open, "once the gate is released the open completes");
            holder.Join();
        }
        finally
        {
            try { dir.Delete(recursive: true); } catch { /* temp */ }
        }
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
}
