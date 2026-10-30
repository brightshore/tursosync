using System.Net;
using System.Net.Sockets;
using Turso.Sync;

namespace TursoSync.Tests;

/// <summary>
/// A push or pull stuck on the network must not stall the rest of the replica. Each test points a synced database at
/// <see cref="HungRemote"/>, a loopback server that reads a request and never answers it, and waits for the request to
/// arrive (a real signal, not a sleep) before checking what the stuck operation still lets through.
/// </summary>
[TestClass]
public class TursoNetworkStallTests
{
    /// <summary>A bound on how long a test waits for something before calling it stuck, so a regression fails instead
    /// of hanging the run. Never a verdict on how fast anything was.</summary>
    private static readonly TimeSpan StuckAfter = TimeSpan.FromSeconds(20);

    /// <summary>What an app can rely on today: a connection opened before the push reads and writes while the push
    /// waits on the network. A push holds no database lock while it waits, only the engine.</summary>
    [TestMethod]
    public async Task An_Open_Connection_Reads_And_Writes_While_A_Push_Is_Stuck_On_The_Network()
    {
        using var remote = HungRemote.Start();
        using var fx = Fixture.Create(remote.BaseUrl);
        using var conn = TursoRawConnection.Open(fx.Db);
        conn.Execute("CREATE TABLE t (x INTEGER PRIMARY KEY)");
        conn.Execute("INSERT INTO t VALUES (1)");

        var push = Task.Factory.StartNew(() => fx.Db.Push(), TaskCreationOptions.LongRunning);
        (await remote.FirstRequest.WaitAsync(StuckAfter)).Should().StartWith("POST", "the push has to be on the network for this to test anything");

        var work = Task.Factory.StartNew(() =>
        {
            conn.Execute("INSERT INTO t VALUES (2)");
            return conn.QueryScalar("SELECT COUNT(*) FROM t");
        }, TaskCreationOptions.LongRunning);

        var first = await Task.WhenAny(work, push, Task.Delay(StuckAfter));
        first.Should().BeSameAs(work, "a push waiting on the network holds no database lock");
        (await work).Should().Be(2L);
        push.IsCompleted.Should().BeFalse("the remote never answered, so the push is still waiting");

        remote.Dispose(); // drop the connection, so the push fails and the fixture can close
        await push.ContinueWith(_ => { });
    }

    /// <summary>
    /// The goal, not yet reachable: opening a NEW connection while a push waits on the network. Blocked in the native
    /// engine, not here. Its sdk-kit wraps the engine in a <c>Mutex&lt;Option&lt;DatabaseSyncEngine&gt;&gt;</c> and every
    /// operation (connect, push, wait_changes, apply, checkpoint) holds it with <c>lock_arc()</c> for its whole life,
    /// network waits included (sync/sdk-kit/src/rsapi.rs at the pinned v0.7.2, and on upstream main). Releasing our
    /// own gate around the HTTP call was tried: the connect then blocked inside the native resume instead, still holding
    /// our gate, and the push deadlocked when its response came back and it needed the gate again. Un-ignore once the
    /// engine lets connect take the engine without waiting for a running push.
    /// </summary>
    [TestMethod]
    [Ignore("Blocked on the native engine: its sdk-kit holds the engine mutex across a push's network wait. See the summary.")]
    public async Task A_New_Connection_Opens_And_Reads_And_Writes_While_A_Push_Is_Stuck_On_The_Network()
    {
        using var remote = HungRemote.Start();
        using var fx = Fixture.Create(remote.BaseUrl);
        using (var seed = TursoRawConnection.Open(fx.Db))
        {
            seed.Execute("CREATE TABLE t (x INTEGER PRIMARY KEY)");
            seed.Execute("INSERT INTO t VALUES (1)");
        }

        var push = Task.Factory.StartNew(() => fx.Db.Push(), TaskCreationOptions.LongRunning);
        (await remote.FirstRequest.WaitAsync(StuckAfter)).Should().StartWith("POST", "the push has to be on the network for this to test anything");

        var work = Task.Factory.StartNew(() =>
        {
            using var conn = TursoRawConnection.Open(fx.Db);
            conn.Execute("INSERT INTO t VALUES (2)");
            return conn.QueryScalar("SELECT COUNT(*) FROM t");
        }, TaskCreationOptions.LongRunning);

        var first = await Task.WhenAny(work, push, Task.Delay(StuckAfter));
        first.Should().BeSameAs(work, "opening a connection and using it needs nothing a push waiting on the network holds");
        (await work).Should().Be(2L);
        push.IsCompleted.Should().BeFalse("the remote never answered, so the push is still waiting");

        remote.Dispose(); // drop the connection, so the push fails and the fixture can close
        await push.ContinueWith(_ => { });
    }

    /// <summary>A push on a remote that never answers fails with the configured Http Timeout, rather than waiting out
    /// HttpClient's 100 s default (which held a consumer's store for up to 130 s on a slow network).</summary>
    [TestMethod]
    public async Task A_Push_To_A_Remote_That_Never_Answers_Fails_With_The_Configured_Http_Timeout()
    {
        using var remote = HungRemote.Start();
        using var fx = Fixture.Create(remote.BaseUrl, c => c with { HttpTimeoutMs = 200 });
        using (var conn = TursoRawConnection.Open(fx.Db))
        {
            conn.Execute("CREATE TABLE t (x INTEGER PRIMARY KEY)");
            conn.Execute("INSERT INTO t VALUES (1)");
        }

        var push = Task.Factory.StartNew(() => fx.Db.Push(), TaskCreationOptions.LongRunning);

        var ex = await push.Invoking(p => p.WaitAsync(StuckAfter)).Should().ThrowAsync<TursoException>(
            "the request runs out of its 200 ms long before the test's bound");
        ex.Which.Message.Should().Contain("Http Timeout");
        remote.Requests.Should().BeGreaterThan(0, "the request reached the remote, so the timeout is what failed it");
    }

    [TestMethod]
    public void Http_Timeout_Is_Read_From_The_Connection_String()
    {
        new TursoConnectionStringBuilder("Data Source=x.db;Http Timeout=1500").ToConfig().HttpTimeoutMs.Should().Be(1500);
        new TursoConnectionStringBuilder("Data Source=x.db;HttpTimeout=-1").ToConfig().HttpTimeoutMs.Should().Be(-1);
        new TursoConnectionStringBuilder("Data Source=x.db").ToConfig().HttpTimeoutMs.Should().Be(0);
    }

    [TestMethod]
    public void Effective_Http_Timeout_Defaults_And_Never_Cuts_A_Long_Poll_Short()
    {
        static TimeSpan Effective(int http, int longPoll = 0) =>
            new TursoSyncConfig { Path = "x.db", HttpTimeoutMs = http, LongPollTimeoutMs = longPoll }.EffectiveHttpTimeout;

        Effective(0).Should().Be(TimeSpan.FromMilliseconds(TursoSyncConfig.DefaultHttpTimeoutMs));
        Effective(250).Should().Be(TimeSpan.FromMilliseconds(250));
        Effective(-1).Should().Be(Timeout.InfiniteTimeSpan);
        Effective(250, longPoll: 90_000).Should().Be(TimeSpan.FromMilliseconds(90_000 + TursoSyncConfig.DefaultHttpTimeoutMs));
        Effective(300_000, longPoll: 90_000).Should().Be(TimeSpan.FromMilliseconds(300_000));
    }

    private sealed class Fixture : IDisposable
    {
        private readonly DirectoryInfo _dir;

        private Fixture(DirectoryInfo dir, TursoSyncDatabase db)
        {
            _dir = dir;
            Db = db;
        }

        public TursoSyncDatabase Db { get; }

        public static Fixture Create(string remoteUrl, Func<TursoSyncConfig, TursoSyncConfig>? configure = null)
        {
            if (!TursoNativeLibrary.IsAvailable())
            {
                Assert.Inconclusive("turso_sync_sdk_kit native library not found");
            }

            var dir = Directory.CreateTempSubdirectory("tursosync-stall-");
            var config = new TursoSyncConfig
            {
                Path = Path.Combine(dir.FullName, "store.db"),
                RemoteUrl = remoteUrl,
                BootstrapIfEmpty = false,
                SchemaGuard = TursoSchemaGuardMode.Off,
            };
            return new Fixture(dir, TursoSyncDatabase.Create(configure?.Invoke(config) ?? config));
        }

        public void Dispose()
        {
            Db.Dispose();
            try { _dir.Delete(recursive: true); } catch { /* temp */ }
        }
    }

    /// <summary>
    /// A loopback remote that accepts every connection, reads the request head, and never answers. Disposing it closes
    /// every connection, which fails whatever is still waiting on one. Raw sockets, like the other stubs here.
    /// </summary>
    internal sealed class HungRemote : IDisposable
    {
        private readonly TcpListener _listener;
        private readonly List<TcpClient> _clients = [];
        private readonly TaskCompletionSource<string> _firstRequest = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _requests;
        private bool _disposed;

        private HungRemote(TcpListener listener)
        {
            _listener = listener;
            BaseUrl = $"http://127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}";
        }

        public string BaseUrl { get; }

        /// <summary>The request line of the first request to arrive.</summary>
        public Task<string> FirstRequest => _firstRequest.Task;

        /// <summary>How many request heads have arrived.</summary>
        public int Requests => Volatile.Read(ref _requests);

        public static HungRemote Start()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var remote = new HungRemote(listener);
            _ = remote.LoopAsync();
            return remote;
        }

        private async Task LoopAsync()
        {
            while (true)
            {
                TcpClient client;
                try
                {
                    client = await _listener.AcceptTcpClientAsync();
                }
                catch
                {
                    return; // stopped
                }

                lock (_clients)
                {
                    if (_disposed)
                    {
                        client.Dispose();
                        return;
                    }

                    _clients.Add(client);
                }

                _ = ReadHeadAsync(client);
            }
        }

        private async Task ReadHeadAsync(TcpClient client)
        {
            try
            {
                var stream = client.GetStream();
                var head = new List<byte>();
                var one = new byte[1];
                while (head.Count < 4 || head[^4] != '\r' || head[^3] != '\n' || head[^2] != '\r' || head[^1] != '\n')
                {
                    if (await stream.ReadAsync(one) == 0)
                    {
                        return;
                    }

                    head.Add(one[0]);
                }

                Interlocked.Increment(ref _requests);
                _firstRequest.TrySetResult(System.Text.Encoding.ASCII.GetString(head.ToArray()).Split("\r\n")[0]);
                // ...and never answer.
            }
            catch
            {
                // closed by Dispose
            }
        }

        public void Dispose()
        {
            lock (_clients)
            {
                if (_disposed)
                {
                    return;
                }

                _disposed = true;
                _listener.Stop();
                foreach (var client in _clients)
                {
                    client.Dispose();
                }
            }
        }
    }
}
