using System.Diagnostics;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Turso.Sync;

namespace TursoSync.Tests;

/// <summary>
/// Live-sync integration tests that exercise the remote <see cref="TursoSyncDatabase"/> path for real —
/// Push, Pull, Stats and Checkpoint plus the whole HTTP IO handler — against a self-hosted Turso sync
/// server (<c>tursodb --sync-server</c>), the same mechanism the Go binding's <c>driver_sync_test.go</c>
/// uses via <c>LOCAL_SYNC_SERVER</c>. These are the package's reason to exist, so they run for real rather
/// than skip when the infra is present.
///
/// Gated on <c>TURSOSYNC_TEST_SYNC_SERVER</c> pointing at a <c>tursodb</c> binary (CI sets it after building the
/// engine; locally, build the CLI and export it). Absent that — or the native — the tests skip (Inconclusive)
/// rather than silently pass. The external-cloud round-trip stays in <see cref="TursoSyncBehaviorTests"/>.
/// </summary>
[TestClass]
public class LiveSyncIntegrationTests
{
    [TestMethod]
    public void LiveSync_LocalWrite_PushPropagatesToRemote()
    {
        using var server = StartServerOrSkip();
        // Parity with TestSyncPush: seed the remote, write locally, confirm the remote is unchanged until Push.
        server.ExecRemote("CREATE TABLE t (x TEXT)", "INSERT INTO t VALUES ('hello'), ('turso'), ('sync-go')");

        var dbPath = TempDb();
        try
        {
            using var db = TursoSyncDatabase.Create(SyncConfig(dbPath, server));
            using var conn = TursoRawConnection.Open(db);

            conn.QueryScalar("SELECT count(*) FROM t").Should().Be(3L);   // bootstrapped from the remote

            conn.Execute("INSERT INTO t VALUES ('push-works')");
            RemoteValues(server, "SELECT x FROM t ORDER BY rowid").Should().Equal("hello", "turso", "sync-go");

            db.Push();

            RemoteValues(server, "SELECT x FROM t ORDER BY rowid")
                .Should().Equal("hello", "turso", "sync-go", "push-works");
        }
        finally
        {
            Cleanup(dbPath);
        }
    }

    [TestMethod]
    public void LiveSync_RemoteWrite_PullAppliesLocally()
    {
        using var server = StartServerOrSkip();
        // Parity with TestSyncPull: a remote write becomes visible locally only after Pull, and a second
        // Pull reports no further changes.
        server.ExecRemote("CREATE TABLE t (x TEXT)", "INSERT INTO t VALUES ('hello'), ('turso'), ('sync-go')");

        var dbPath = TempDb();
        try
        {
            using var db = TursoSyncDatabase.Create(SyncConfig(dbPath, server));
            using var conn = TursoRawConnection.Open(db);

            conn.QueryScalar("SELECT count(*) FROM t").Should().Be(3L);

            server.ExecRemote("INSERT INTO t VALUES ('pull-works')");

            db.Pull().Should().BeTrue();    // the remote change is applied
            db.Pull().Should().BeFalse();   // nothing left to pull

            conn.QueryScalar("SELECT x FROM t WHERE x = 'pull-works'").Should().Be("pull-works");
            conn.QueryScalar("SELECT count(*) FROM t").Should().Be(4L);
        }
        finally
        {
            Cleanup(dbPath);
        }
    }

    [TestMethod]
    public void LiveSync_Checkpoint_TruncatesWal_AndStatsReflectIt()
    {
        using var server = StartServerOrSkip();

        var dbPath = TempDb();
        try
        {
            // Parity with TestSyncCheckpoint: fill the WAL, checkpoint it away, then push the result remotely.
            using var db = TursoSyncDatabase.Create(SyncConfig(dbPath, server));
            using (var conn = TursoRawConnection.Open(db))
            {
                conn.Execute("CREATE TABLE t (x INTEGER)");
                for (var i = 0; i < 1024; i++)
                {
                    conn.Execute($"INSERT INTO t VALUES ({i})");
                }
            }

            var before = db.Stats();
            before.MainWalSize.Should().BeGreaterThan(1024 * 1024);
            before.RevertWalSize.Should().Be(0);

            db.Checkpoint();

            var after = db.Stats();
            after.MainWalSize.Should().Be(0);
            after.RevertWalSize.Should().BeLessThan(8 * 1024);

            db.Push();

            const long expectedSum = 1024L * 1023L / 2L;
            RemoteValues(server, "SELECT sum(x) FROM t").Should().Equal(expectedSum.ToString());
        }
        finally
        {
            Cleanup(dbPath);
        }
    }

    // ---- pre-attach strand + repair (the Tweed thread_checkpoint incident, generalized) ----------

    [TestMethod]
    public void LiveSync_Reconcile_TeachesServer_A_PreAttach_Table()
    {
        using var server = StartServerOrSkip();
        var dbPath = TempDb();
        try
        {
            // A local-only database with pre-attach history — the stranded state: the engine only
            // replicates changes recorded after the remote is bound, so this table's DDL + rows are
            // invisible to push.
            using (var conn = TursoRawConnection.OpenLocal(new TursoSyncConfig { Path = dbPath }))
            {
                conn.Execute("CREATE TABLE pre (x TEXT)");
                conn.Execute("CREATE INDEX ix_pre ON pre (x)");
                conn.Execute("INSERT INTO pre VALUES ('a'), ('b'), ('c')");
            }

            // Attach the remote to the EXISTING file. Guard off in this arm: it observes and repairs the
            // strand rather than asserting how this engine version treats the attach itself.
            using var db = TursoSyncDatabase.Create(SyncConfig(dbPath, server) with
            {
                BootstrapIfEmpty = false,
                SchemaGuard = TursoSchemaGuardMode.Off,
            });
            using (var conn = TursoRawConnection.Open(db))
            {
                if (!TursoSchemaGuard.UserTables(conn).Contains("pre"))
                {
                    Assert.Inconclusive("this engine version already dropped the pre-attach table at attach");
                }
            }

            db.Push();

            var report = db.ReconcileLocalTables();
            report.RebuiltTables.Should().Contain("pre");
            report.SkippedTables.Should().BeEmpty();
            report.RowsCopied.Should().BeGreaterThanOrEqualTo(3);

            // The proof: a FRESH replica bootstrapped from the remote now has the table, its rows, and its
            // index — the server finally learned schema + data that predate the attach.
            var freshPath = TempDb();
            try
            {
                using var fresh = TursoSyncDatabase.Create(SyncConfig(freshPath, server));
                using var conn = TursoRawConnection.Open(fresh);
                conn.QueryScalar("SELECT count(*) FROM pre").Should().Be(3L);
                conn.QueryScalar("SELECT count(*) FROM sqlite_master WHERE type='index' AND name='ix_pre'")
                    .Should().Be(1L);
            }
            finally
            {
                Cleanup(freshPath);
            }
        }
        finally
        {
            Cleanup(dbPath);
        }
    }

    [TestMethod]
    public void LiveSync_SchemaGuard_On_Attach_Either_Passes_Or_Detects_With_Backup()
    {
        using var server = StartServerOrSkip();
        var dbPath = TempDb();
        try
        {
            using (var conn = TursoRawConnection.OpenLocal(new TursoSyncConfig { Path = dbPath }))
            {
                conn.Execute("CREATE TABLE pre (x TEXT)");
                conn.Execute("INSERT INTO pre VALUES ('guarded')");
            }

            // Attaching an existing replica is exactly the operation the guard watches. Whichever way this
            // engine version behaves, the outcome must be safe: either the table survives the attach, or
            // the guard throws — naming the table and holding a pre-attach backup it can be recovered from.
            try
            {
                using var db = TursoSyncDatabase.Create(SyncConfig(dbPath, server) with
                {
                    BootstrapIfEmpty = false,
                    SchemaGuard = TursoSchemaGuardMode.DetectAndBackup,
                });
                using var conn = TursoRawConnection.Open(db);
                TursoSchemaGuard.UserTables(conn).Should().Contain("pre");
            }
            catch (TursoSchemaGuardException ex)
            {
                ex.Operation.Should().Be("create");
                ex.DroppedTables.Should().Contain("pre");
                ex.BackupDirectory.Should().NotBeNull();
                TursoSchemaGuard.UserTablesOfCopy(Path.Combine(ex.BackupDirectory!, Path.GetFileName(dbPath)))
                    .Should().Contain("pre");
            }
        }
        finally
        {
            Cleanup(dbPath);
        }
    }

    [TestMethod]
    public void LiveSync_Pull_With_Guard_Passes_When_Nothing_Is_Dropped()
    {
        using var server = StartServerOrSkip();
        server.ExecRemote("CREATE TABLE t (x TEXT)", "INSERT INTO t VALUES ('one')");

        var dbPath = TempDb();
        try
        {
            using var db = TursoSyncDatabase.Create(SyncConfig(dbPath, server)); // guard defaults to Detect
            server.ExecRemote("CREATE TABLE added_later (y TEXT)", "INSERT INTO t VALUES ('two')");
            db.Pull(); // additions must pass the guard silently
            using var conn = TursoRawConnection.Open(db);
            conn.QueryScalar("SELECT count(*) FROM t").Should().Be(2L);
            TursoSchemaGuard.UserTables(conn).Should().Contain("added_later");
        }
        finally
        {
            Cleanup(dbPath);
        }
    }

    /// <summary>
    /// A pull on a slow link holds the sync engine for its whole download, and a new connection waits for it to end.
    /// Connections warmed beforehand are rented from the pool instead, and their statements run during the download.
    /// The server's responses are throttled through a proxy so the pull is genuinely in progress while the test opens.
    /// </summary>
    [TestMethod]
    public async Task LiveSync_WarmedPool_Opens_During_A_Slow_Pull()
    {
        using var server = StartServerOrSkip();
        server.ExecRemote("CREATE TABLE big (x TEXT)", "INSERT INTO big VALUES ('seed')");
        using var proxy = new ThrottlingProxy(new Uri(server.BaseUrl).Port);

        var dbPath = TempDb();
        var connString = $"Data Source={dbPath};Remote Url={proxy.BaseUrl};Bootstrap=true;Max Idle Connections=4";
        try
        {
            TursoConnection.WarmPool(connString, 4).Should().Be(4);
            TursoSyncDatabase db;
            using (var first = new TursoConnection(connString))
            {
                first.Open();
                db = first.SyncDatabase!;
            }

            // About 1 MB of remote changes, sent back at 150 KB/s: a pull of several seconds.
            server.ExecRemote(Enumerable.Range(0, 200).Select(_ => "INSERT INTO big VALUES (hex(randomblob(2500)))").ToArray());
            proxy.BytesPerSecond = 150_000;
            var pull = Task.Run(db.Pull);
            await WaitUntil(() => proxy.SentToClient > 50_000, TimeSpan.FromSeconds(20));

            var held = new List<TursoConnection>();
            try
            {
                var watch = Stopwatch.StartNew();
                for (var i = 0; i < 4; i++)
                {
                    var c = new TursoConnection(connString);
                    held.Add(c);
                    c.Open();
                    using var cmd = c.CreateCommand();
                    cmd.CommandText = "SELECT count(*) FROM big";
                    cmd.ExecuteScalar().Should().Be(1L, "the pull has not applied yet");
                }

                pull.IsCompleted.Should().BeFalse("the opens must have happened during the download for this to mean anything");
                watch.ElapsedMilliseconds.Should().BeLessThan(1000, "a warmed connection does not wait for the pull");

                using var cold = new TursoConnection(connString);
                var opening = cold.OpenAsync();
                await Task.Delay(500);
                var opened = opening.IsCompleted;
                // Read AFTER the open's status: a pull still running now was running when that status was read.
                pull.IsCompleted.Should().BeFalse("the cold open must be checked while the download is still running");
                opened.Should().BeFalse("a fifth, cold open waits for the pull to end");
                await opening.WaitAsync(TimeSpan.FromSeconds(45));
            }
            finally
            {
                foreach (var c in held)
                {
                    c.Dispose();
                }
            }

            (await pull.WaitAsync(TimeSpan.FromSeconds(45))).Should().BeTrue();
        }
        finally
        {
            TursoConnection.ClearPool();
            Cleanup(dbPath);
        }
    }

    // ---- harness ---------------------------------------------------------------------------------

    private static async Task WaitUntil(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = Stopwatch.StartNew();
        while (!condition())
        {
            deadline.Elapsed.Should().BeLessThan(timeout, "the condition should have come true");
            await Task.Delay(20);
        }
    }

    /// <summary>A TCP proxy in front of the sync server whose responses can be throttled, to make a pull slow.</summary>
    private sealed class ThrottlingProxy : IDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly int _targetPort;
        private readonly CancellationTokenSource _cts = new();
        private long _sentToClient;

        public ThrottlingProxy(int targetPort)
        {
            _targetPort = targetPort;
            _listener.Start();
            _ = AcceptLoopAsync();
        }

        public string BaseUrl => $"http://localhost:{((IPEndPoint)_listener.LocalEndpoint).Port}";

        /// <summary>The server-to-client rate; 0 is unthrottled.</summary>
        public volatile int BytesPerSecond;

        public long SentToClient => Interlocked.Read(ref _sentToClient);

        private async Task AcceptLoopAsync()
        {
            while (!_cts.IsCancellationRequested)
            {
                TcpClient client;
                try
                {
                    client = await _listener.AcceptTcpClientAsync(_cts.Token);
                }
                catch
                {
                    return; // stopping
                }

                _ = RelayAsync(client);
            }
        }

        private async Task RelayAsync(TcpClient client)
        {
            using (client)
            using (var upstream = new TcpClient())
            {
                try
                {
                    await upstream.ConnectAsync(IPAddress.Loopback, _targetPort);
                    var toServer = client.GetStream().CopyToAsync(upstream.GetStream(), _cts.Token);
                    var toClient = CopyThrottledAsync(upstream.GetStream(), client.GetStream());
                    await Task.WhenAny(toServer, toClient);
                }
                catch
                {
                    // a connection the client or the test tore down
                }
            }
        }

        private async Task CopyThrottledAsync(Stream from, Stream to)
        {
            var buffer = new byte[8192];
            int read;
            while ((read = await from.ReadAsync(buffer, _cts.Token)) > 0)
            {
                if (BytesPerSecond is > 0 and var rate)
                {
                    await Task.Delay(TimeSpan.FromSeconds((double)read / rate), _cts.Token);
                }

                await to.WriteAsync(buffer.AsMemory(0, read), _cts.Token);
                Interlocked.Add(ref _sentToClient, read);
            }
        }

        public void Dispose()
        {
            _cts.Cancel();
            _listener.Stop();
        }
    }

    private static TursoSyncConfig SyncConfig(string path, LocalSyncServer server) =>
        new() { Path = path, RemoteUrl = server.BaseUrl, BootstrapIfEmpty = true };

    private static List<object?> RemoteValues(LocalSyncServer server, string sql) =>
        server.ExecRemote(sql).Select(row => row[0]).ToList();

    private static LocalSyncServer StartServerOrSkip()
    {
        if (!TursoNativeLibrary.IsAvailable())
        {
            Assert.Inconclusive("turso_sync_sdk_kit native library not found");
        }

        var server = LocalSyncServer.TryStart();
        if (server is null)
        {
            Assert.Inconclusive(
                "Set TURSOSYNC_TEST_SYNC_SERVER to a `tursodb` binary to run the live-sync integration tests.");
        }

        return server!;
    }

    private static string TempDb()
    {
        var dir = Path.Combine(Path.GetTempPath(), "tursosync-livesync-" + Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(dir);
        return Path.Combine(dir, "store.db");
    }

    private static void Cleanup(string dbPath)
    {
        var dir = Path.GetDirectoryName(dbPath);
        if (dir is not null && Directory.Exists(dir))
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    /// <summary>
    /// A throwaway <c>tursodb --sync-server</c> process on a free port, with its own working directory so
    /// each test gets an isolated remote. Mirrors the Go test harness's local-server path.
    /// </summary>
    private sealed class LocalSyncServer : IDisposable
    {
        private readonly int _port;
        private readonly string _workDir;
        private readonly StringBuilder _stderr = new();
        private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(30) };
        private Process? _process;

        private LocalSyncServer(int port, string workDir)
        {
            _port = port;
            _workDir = workDir;
        }

        public string BaseUrl => $"http://localhost:{_port}";

        /// <summary>Start a server, or return null when <c>TURSOSYNC_TEST_SYNC_SERVER</c> is unset/missing.</summary>
        public static LocalSyncServer? TryStart()
        {
            var bin = Environment.GetEnvironmentVariable("TURSOSYNC_TEST_SYNC_SERVER");
            if (string.IsNullOrWhiteSpace(bin) || !File.Exists(bin))
            {
                return null;
            }

            Exception? last = null;
            for (var attempt = 0; attempt < 5; attempt++)
            {
                var workDir = Path.Combine(Path.GetTempPath(), "tursosync-server-" + Guid.NewGuid().ToString("n"));
                Directory.CreateDirectory(workDir);
                var server = new LocalSyncServer(FreePort(), workDir);
                try
                {
                    server.Start(bin);
                    server.WaitUntilReady(TimeSpan.FromSeconds(30));
                    return server;
                }
                catch (Exception ex)
                {
                    last = ex;
                    server.Dispose();
                }
            }

            throw new InvalidOperationException("tursodb sync server failed to start after 5 attempts", last);
        }

        /// <summary>Run SQL directly against the remote over the Hrana <c>/v2/pipeline</c> endpoint.</summary>
        public IReadOnlyList<IReadOnlyList<object?>> ExecRemote(params string[] sqls)
        {
            var requests = sqls.Select(sql => new { type = "execute", stmt = new { sql } }).ToArray();
            var payload = JsonSerializer.Serialize(new { requests });
            using var content = new StringContent(payload, Encoding.UTF8, "application/json");
            using var response = _http.PostAsync($"{BaseUrl}/v2/pipeline", content).GetAwaiter().GetResult();
            var body = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
            if (!response.IsSuccessStatusCode)
            {
                throw new InvalidOperationException($"pipeline HTTP {(int)response.StatusCode}: {body}");
            }

            using var doc = JsonDocument.Parse(body);
            var results = doc.RootElement.GetProperty("results");
            var last = results[results.GetArrayLength() - 1];
            if (last.GetProperty("type").GetString() != "ok")
            {
                throw new InvalidOperationException($"pipeline statement error: {body}");
            }

            var rows = new List<IReadOnlyList<object?>>();
            foreach (var row in last.GetProperty("response").GetProperty("result").GetProperty("rows").EnumerateArray())
            {
                var cells = new List<object?>();
                foreach (var cell in row.EnumerateArray())
                {
                    cells.Add(ParseCell(cell));
                }

                rows.Add(cells);
            }

            return rows;
        }

        public void Dispose()
        {
            if (_process is { } process)
            {
                try
                {
                    if (!process.HasExited)
                    {
                        process.Kill(entireProcessTree: true);
                        process.WaitForExit(5000);
                    }
                }
                catch
                {
                    // best-effort teardown
                }

                process.Dispose();
                _process = null;
            }

            _http.Dispose();

            try
            {
                if (Directory.Exists(_workDir))
                {
                    Directory.Delete(_workDir, recursive: true);
                }
            }
            catch
            {
                // best-effort cleanup
            }
        }

        private void Start(string bin)
        {
            var process = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = bin,
                    ArgumentList = { "--sync-server", $"0.0.0.0:{_port}" },
                    WorkingDirectory = _workDir,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                },
            };
            // Drain both pipes so a chatty server can never block on a full buffer.
            process.OutputDataReceived += (_, _) => { };
            process.ErrorDataReceived += (_, e) =>
            {
                if (e.Data is not null)
                {
                    lock (_stderr)
                    {
                        _stderr.AppendLine(e.Data);
                    }
                }
            };
            process.Start();
            process.BeginOutputReadLine();
            process.BeginErrorReadLine();
            _process = process;
        }

        private void WaitUntilReady(TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow + timeout;
            while (DateTime.UtcNow < deadline)
            {
                if (_process!.HasExited)
                {
                    throw new InvalidOperationException(
                        $"sync server exited early (code {_process.ExitCode}): {StderrSnapshot()}");
                }

                try
                {
                    using var response = _http.GetAsync(BaseUrl).GetAwaiter().GetResult();
                    return; // any HTTP response means the listener is up
                }
                catch (HttpRequestException)
                {
                    Thread.Sleep(50);
                }
            }

            throw new TimeoutException($"sync server not ready within {timeout.TotalSeconds:0}s: {StderrSnapshot()}");
        }

        private string StderrSnapshot()
        {
            lock (_stderr)
            {
                return _stderr.ToString();
            }
        }

        private static int FreePort()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            try
            {
                return ((IPEndPoint)listener.LocalEndpoint).Port;
            }
            finally
            {
                listener.Stop();
            }
        }

        private static object? ParseCell(JsonElement cell)
        {
            if (cell.GetProperty("type").GetString() == "null" || !cell.TryGetProperty("value", out var value))
            {
                return null;
            }

            return value.ValueKind == JsonValueKind.String ? value.GetString() : value.ToString();
        }
    }
}
