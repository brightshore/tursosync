using System.Diagnostics;
using Turso.Sync;

namespace TursoSync.Tests;

/// <summary>
/// <see cref="TursoConnection.WarmPool"/> leaves idle connections that open without the sync database's gate, which a
/// push or pull holds for its whole round trip. Each test holds the gate on another thread, standing in for a slow
/// pull. The real thing, against a throttled sync server, is
/// <see cref="LiveSyncIntegrationTests.LiveSync_WarmedPool_Opens_During_A_Slow_Pull"/>.
/// </summary>
[TestClass]
public class TursoWarmPoolTests
{
    private const int GateHeldMs = 1500;

    [TestMethod]
    public async Task Warmed_Connections_Open_While_The_Gate_Is_Held_And_The_Next_Waits()
    {
        using var fx = Fixture.Create("Max Idle Connections=4");
        TursoConnection.WarmPool(fx.ConnString, 4).Should().Be(4);

        using (fx.HoldGate())
        {
            var held = new List<TursoConnection>();
            try
            {
                var watch = Stopwatch.StartNew();
                for (var i = 0; i < 4; i++)
                {
                    var c = new TursoConnection(fx.ConnString);
                    held.Add(c);
                    c.Open();
                    using var cmd = c.CreateCommand();
                    cmd.CommandText = "SELECT 1";
                    cmd.ExecuteScalar().Should().Be(1L);
                }

                watch.ElapsedMilliseconds.Should().BeLessThan(GateHeldMs / 4,
                    "a warmed connection is rented from the pool, which needs nothing the gate guards");

                using var fifth = new TursoConnection(fx.ConnString);
                var opening = fifth.OpenAsync();
                await Task.Delay(GateHeldMs / 4);
                opening.IsCompleted.Should().BeFalse("with the four warmed connections in use, the fifth has to connect, and that waits for the gate");
                await opening.WaitAsync(TimeSpan.FromSeconds(30));
            }
            finally
            {
                foreach (var c in held)
                {
                    c.Dispose();
                }
            }
        }
    }

    [TestMethod]
    public void Warming_Twice_Opens_Nothing_New_And_Is_Capped_At_Max_Idle()
    {
        using var fx = Fixture.Create("Max Idle Connections=3");

        TursoConnection.WarmPool(fx.ConnString, 10).Should().Be(3, "the pool would drop anything returned beyond Max Idle Connections");
        TursoConnection.WarmPool(fx.ConnString, 2).Should().Be(3, "the idle ones are reused, not added to");
    }

    [TestMethod]
    public void Warming_Does_Nothing_When_Pooling_Is_Off()
    {
        using var fx = Fixture.Create("Pooling=false");

        TursoConnection.WarmPool(fx.ConnString, 4).Should().Be(0);
        TursoConnection.OpenReplicas.Should().NotContain(r => r.Path == fx.DbPath, "nothing was left open");
    }

    private sealed class Fixture : IDisposable
    {
        private readonly DirectoryInfo _dir;

        private Fixture(DirectoryInfo dir, string extra)
        {
            _dir = dir;
            ConnString = $"Data Source={DbPath};Sync=true;Bootstrap=false;{extra}";
        }

        public string DbPath => Path.Combine(_dir.FullName, "store.db");

        public string ConnString { get; }

        public static Fixture Create(string extra)
        {
            if (!TursoNativeLibrary.IsAvailable())
            {
                Assert.Inconclusive("turso_sync_sdk_kit native library not found");
            }

            return new Fixture(Directory.CreateTempSubdirectory("tursosync-warm-pool-"), extra);
        }

        /// <summary>Hold the replica's sync gate on another thread for <see cref="GateHeldMs"/>, as a pull would.</summary>
        public IDisposable HoldGate()
        {
            var db = new TursoConnection(ConnString);
            db.Open();
            var gate = db.SyncDatabase?.Gate ?? throw new AssertFailedException("the sync lane should have a sync database");
            db.Dispose(); // back to the pool; the engine (and its gate) is shared by every connection to this replica

            using var held = new ManualResetEventSlim();
            var holder = new Thread(() =>
            {
                lock (gate)
                {
                    held.Set();
                    Thread.Sleep(GateHeldMs);
                }
            }) { IsBackground = true };
            holder.Start();
            held.Wait();
            return new Joiner(holder);
        }

        public void Dispose()
        {
            TursoConnection.ClearPool();
            try { _dir.Delete(recursive: true); } catch { /* temp */ }
        }

        private sealed class Joiner(Thread thread) : IDisposable
        {
            public void Dispose() => thread.Join();
        }
    }
}
