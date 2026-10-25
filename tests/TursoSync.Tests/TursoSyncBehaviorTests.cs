using System.Net;
using System.Net.Sockets;
using Turso.Sync;

namespace TursoSync.Tests;

/// <summary>
/// Behaviors ported from the Go binding's <c>driver_sync_test.go</c>. The config-surface and local-engine
/// cases run offline; the remote round-trip cases (Push/Pull) mirror the Go tests but need a live Turso
/// sync server, so they are gated on <c>TURSOSYNC_TEST_REMOTE_URL</c>/<c>TURSOSYNC_TEST_REMOTE_TOKEN</c> (same pattern
/// as the Neon-gated Postgres tests) and skip — not silently pass — when that infra is absent.
/// </summary>
[TestClass]
public class TursoSyncBehaviorTests
{
    // ---- config surface (parity with TestSyncDSNParsing / TestSyncBusyTimeoutConfigPrecedence) ------

    [TestMethod]
    public void Config_DefaultsBusyTimeout()
    {
        var config = new TursoConnectionStringBuilder("Data Source=mydb.db").ToConfig();
        config.Path.Should().Be("mydb.db");
        config.BusyTimeoutMs.Should().Be(5000);
        config.BootstrapIfEmpty.Should().BeFalse();
        config.RemoteUrl.Should().BeNull();
    }

    [TestMethod]
    public void Config_ExplicitBusyTimeout_Wins()
    {
        new TursoConnectionStringBuilder("Data Source=mydb.db;Busy Timeout=10000").ToConfig()
            .BusyTimeoutMs.Should().Be(10000);
    }

    [TestMethod]
    public void Config_NegativeBusyTimeout_DisablesIt()
    {
        new TursoConnectionStringBuilder("Data Source=mydb.db;Busy Timeout=-1").ToConfig()
            .BusyTimeoutMs.Should().Be(-1);
    }

    [TestMethod]
    public void Config_SyncFields_RoundTrip()
    {
        var config = new TursoConnectionStringBuilder(
                "Data Source=x.db;Remote Url=libsql://host;Auth Token=tok;Namespace=ns;Bootstrap=true")
            .ToConfig();
        config.RemoteUrl.Should().Be("libsql://host");
        config.AuthToken.Should().Be("tok");
        config.Namespace.Should().Be("ns");
        config.BootstrapIfEmpty.Should().BeTrue();
    }

    [TestMethod]
    public void Config_KeywordAliases_AreAccepted()
    {
        var config = new TursoConnectionStringBuilder("DataSource=a.db;RemoteUrl=libsql://h;AuthToken=t;BusyTimeout=2000")
            .ToConfig();
        config.Path.Should().Be("a.db");
        config.RemoteUrl.Should().Be("libsql://h");
        config.AuthToken.Should().Be("t");
        config.BusyTimeoutMs.Should().Be(2000);
    }

    [TestMethod]
    public void Config_MissingDataSource_Throws()
    {
        var act = () => new TursoConnectionStringBuilder("Busy Timeout=1000").ToConfig();
        act.Should().Throw<ArgumentException>();
    }

    // ---- encryption is a base-engine feature; the sync engine must reject it -----------------------

    [TestMethod]
    public void Create_WithLocalEncryption_OnSyncEngine_Throws()
    {
        // Local at-rest encryption isn't supported on the sync lane (the Go binding never plumbed it, and the
        // engine can't reopen the encrypted local file). Create must reject it loudly — deterministically,
        // before any native call — rather than hand back a database that's lost on the next open.
        var config = new TursoSyncConfig
        {
            Path = "ignored.db",
            RemoteUrl = "libsql://example",
            EncryptionCipher = TursoEncryptionCipher.Aes256Gcm.ToName(),
            EncryptionKey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        };

        var act = () => TursoSyncDatabase.Create(config);
        act.Should().Throw<NotSupportedException>().WithMessage("*base engine*");
    }

    // ---- local engine behaviors (Stats / Checkpoint work without a remote) -------------------------

    [TestMethod]
    public void Stats_OnLocalDb_Succeeds()
    {
        if (!TursoNativeLibrary.IsAvailable())
        {
            Assert.Inconclusive("turso_sync_sdk_kit native library not found");
            return;
        }

        var dbPath = TempDb();
        try
        {
            using var db = TursoSyncDatabase.Create(new TursoSyncConfig { Path = dbPath, BootstrapIfEmpty = false });
            using var conn = TursoRawConnection.Open(db);
            conn.Execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
            conn.Execute("INSERT INTO t (id, v) VALUES (1, 'a')");

            var stats = db.Stats();
            stats.Should().NotBeNull();
            stats.MainWalSize.Should().BeGreaterThanOrEqualTo(0);
        }
        finally
        {
            Cleanup(dbPath);
        }
    }

    [TestMethod]
    public void Checkpoint_OnLocalDb_Succeeds()
    {
        if (!TursoNativeLibrary.IsAvailable())
        {
            Assert.Inconclusive("turso_sync_sdk_kit native library not found");
            return;
        }

        var dbPath = TempDb();
        try
        {
            using var db = TursoSyncDatabase.Create(new TursoSyncConfig { Path = dbPath, BootstrapIfEmpty = false });
            using (var conn = TursoRawConnection.Open(db))
            {
                conn.Execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
            }

            var act = db.Checkpoint;
            act.Should().NotThrow();
        }
        finally
        {
            Cleanup(dbPath);
        }
    }

    // ---- remote round-trip (parity with TestSyncPush / TestSyncPull), gated on a live sync server ---

    [TestMethod]
    public void PushPull_RoundTripsThroughRemote()
    {
        if (!TursoNativeLibrary.IsAvailable())
        {
            Assert.Inconclusive("turso_sync_sdk_kit native library not found");
            return;
        }

        var url = Environment.GetEnvironmentVariable("TURSOSYNC_TEST_REMOTE_URL");
        var token = Environment.GetEnvironmentVariable("TURSOSYNC_TEST_REMOTE_TOKEN");
        if (string.IsNullOrWhiteSpace(url))
        {
            Assert.Inconclusive("Set TURSOSYNC_TEST_REMOTE_URL (+ TURSOSYNC_TEST_REMOTE_TOKEN) to run the remote sync round-trip.");
            return;
        }

        var writerPath = TempDb();
        var readerPath = TempDb();
        // A unique key per run so the test is repeatable against a persistent remote (a real sync database
        // accumulates rows; it is not truncated between runs) rather than assuming a pristine remote.
        var key = "hello-" + Guid.NewGuid().ToString("n");
        try
        {
            using (var writer = TursoSyncDatabase.Create(new TursoSyncConfig { Path = writerPath, RemoteUrl = url, AuthToken = token, BootstrapIfEmpty = true }))
            {
                using (var conn = TursoRawConnection.Open(writer))
                {
                    conn.Execute("CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT)");
                    conn.Execute($"INSERT INTO kv (k, v) VALUES ('{key}', 'world')");
                }

                writer.Push();
            }

            using var reader = TursoSyncDatabase.Create(new TursoSyncConfig { Path = readerPath, RemoteUrl = url, AuthToken = token, BootstrapIfEmpty = true });
            reader.Pull();
            using var readConn = TursoRawConnection.Open(reader);
            readConn.QueryScalar($"SELECT v FROM kv WHERE k = '{key}'").Should().Be("world");
        }
        finally
        {
            Cleanup(writerPath);
            Cleanup(readerPath);
        }
    }

    [TestMethod]
    public void RemoteSync_BadAuthToken_FailsClearly()
    {
        var (url, _) = RemoteOrSkip();

        // A bogus token must surface as a loud error (the remote rejects /info with 401), not a hang or a
        // silently-empty database. Guards the Authorization header path in the HTTP IO handler.
        var dbPath = TempDb();
        try
        {
            var act = () => TursoSyncDatabase.Create(new TursoSyncConfig
            {
                Path = dbPath,
                RemoteUrl = url,
                AuthToken = "invalid.invalid.invalid",
                BootstrapIfEmpty = true,
            });
            act.Should().Throw<TursoException>();
        }
        finally
        {
            Cleanup(dbPath);
        }
    }

    [TestMethod]
    public void RemoteSync_UpdateAndDelete_Propagate()
    {
        var (url, token) = RemoteOrSkip();

        // The round-trip test only covers INSERT; UPDATE and DELETE must sync too. A unique table keeps the
        // test idempotent against the persistent remote, and it is dropped (and the drop pushed) at the end.
        var table = "sync_ud_" + Guid.NewGuid().ToString("n");
        var writerPath = TempDb();
        var readerPath = TempDb();
        try
        {
            using (var writer = TursoSyncDatabase.Create(RemoteConfig(writerPath, url, token)))
            {
                using (var conn = TursoRawConnection.Open(writer))
                {
                    conn.Execute($"CREATE TABLE {table} (k INTEGER PRIMARY KEY, v TEXT)");
                    conn.Execute($"INSERT INTO {table} (k, v) VALUES (1, 'one'), (2, 'two'), (3, 'three')");
                    conn.Execute($"UPDATE {table} SET v = 'TWO' WHERE k = 2");
                    conn.Execute($"DELETE FROM {table} WHERE k = 3");
                }

                writer.Push();
            }

            using var reader = TursoSyncDatabase.Create(RemoteConfig(readerPath, url, token));
            reader.Pull();
            using var readConn = TursoRawConnection.Open(reader);
            readConn.QueryScalar($"SELECT v FROM {table} WHERE k = 1").Should().Be("one");
            readConn.QueryScalar($"SELECT v FROM {table} WHERE k = 2").Should().Be("TWO");
            readConn.QueryScalar($"SELECT count(*) FROM {table}").Should().Be(2L);
        }
        finally
        {
            DropRemoteTable(url, token, table);
            Cleanup(writerPath);
            Cleanup(readerPath);
        }
    }

    [TestMethod]
    public void RemoteSync_ValueTypes_RoundTrip()
    {
        var (url, token) = RemoteOrSkip();

        // Integer, real, text, NULL and BLOB must survive the sync encoding intact, not just short ASCII text.
        var table = "sync_vt_" + Guid.NewGuid().ToString("n");
        var blob = new byte[] { 0, 1, 2, 253, 254, 255 };
        var bigText = new string('x', 100_000);
        var writerPath = TempDb();
        var readerPath = TempDb();
        try
        {
            using (var writer = TursoSyncDatabase.Create(RemoteConfig(writerPath, url, token)))
            {
                using (var conn = TursoRawConnection.Open(writer))
                {
                    conn.Execute($"CREATE TABLE {table} (i INTEGER, r REAL, t TEXT, n TEXT, b BLOB, big TEXT)");
                    conn.Execute(
                        $"INSERT INTO {table} (i, r, t, n, b, big) VALUES " +
                        $"(42, 3.5, 'hello', NULL, x'000102fdfeff', '{bigText}')");
                }

                writer.Push();
            }

            using var reader = TursoSyncDatabase.Create(RemoteConfig(readerPath, url, token));
            reader.Pull();
            using var readConn = TursoRawConnection.Open(reader);
            readConn.QueryScalar($"SELECT i FROM {table}").Should().Be(42L);
            readConn.QueryScalar($"SELECT r FROM {table}").Should().Be(3.5);
            readConn.QueryScalar($"SELECT t FROM {table}").Should().Be("hello");
            readConn.QueryScalar($"SELECT n FROM {table}").Should().BeNull();
            readConn.QueryScalar($"SELECT b FROM {table}").Should().BeEquivalentTo(blob);
            readConn.QueryScalar($"SELECT big FROM {table}").Should().Be(bigText);
        }
        finally
        {
            DropRemoteTable(url, token, table);
            Cleanup(writerPath);
            Cleanup(readerPath);
        }
    }

    [TestMethod]
    public void RemoteSync_ReopenExistingLocalDb_Resumes()
    {
        var (url, token) = RemoteOrSkip();

        // Reopening an existing synced local database (BootstrapIfEmpty=false) must resume from the persisted
        // local state rather than re-bootstrap — a distinct engine path from the fresh-bootstrap round-trip.
        var table = "sync_re_" + Guid.NewGuid().ToString("n");
        var dbPath = TempDb();
        try
        {
            using (var db = TursoSyncDatabase.Create(RemoteConfig(dbPath, url, token)))
            {
                using (var conn = TursoRawConnection.Open(db))
                {
                    conn.Execute($"CREATE TABLE {table} (k INTEGER PRIMARY KEY, v TEXT)");
                    conn.Execute($"INSERT INTO {table} (k, v) VALUES (1, 'first')");
                }

                db.Push();
            }

            // Reopen the same local directory without bootstrapping; the row must still be present locally.
            using (var reopened = TursoSyncDatabase.Create(
                new TursoSyncConfig { Path = dbPath, RemoteUrl = url, AuthToken = token, BootstrapIfEmpty = false }))
            {
                using var conn = TursoRawConnection.Open(reopened);
                conn.QueryScalar($"SELECT v FROM {table} WHERE k = 1").Should().Be("first");

                conn.Execute($"INSERT INTO {table} (k, v) VALUES (2, 'second')");
                reopened.Push();
                conn.QueryScalar($"SELECT count(*) FROM {table}").Should().Be(2L);
            }
        }
        finally
        {
            DropRemoteTable(url, token, table);
            Cleanup(dbPath);
        }
    }

    [TestMethod]
    public void RemoteSync_LargeDataset_ChunkedBootstrap_RoundTrips()
    {
        var (url, token) = RemoteOrSkip();

        // Push a multi-hundred-KB database, then bootstrap a fresh reader with a small PullBytesThreshold so
        // the download is chunked into several /pull-updates requests. Exercises both the chunked-bootstrap
        // path and the C# response-stream reassembly (PumpStream); a chunking or reassembly bug corrupts or
        // loses rows, which the count + checksum catch.
        var table = "sync_big_" + Guid.NewGuid().ToString("n");
        const int rows = 3000;
        var payload = new string('p', 300);
        var writerPath = TempDb();
        var readerPath = TempDb();
        try
        {
            using (var writer = TursoSyncDatabase.Create(RemoteConfig(writerPath, url, token)))
            {
                using (var conn = TursoRawConnection.Open(writer))
                {
                    conn.Execute($"CREATE TABLE {table} (id INTEGER PRIMARY KEY, payload TEXT)");
                    for (var i = 1; i <= rows; i++)
                    {
                        conn.Execute($"INSERT INTO {table} (id, payload) VALUES ({i}, '{payload}')");
                    }
                }

                writer.Push();
            }

            // 64 KB threshold over a ~1 MB database => many chunks.
            var reader = TursoSyncDatabase.Create(new TursoSyncConfig
            {
                Path = readerPath,
                RemoteUrl = url,
                AuthToken = token,
                BootstrapIfEmpty = true,
                PullBytesThreshold = 64 * 1024,
            });
            using (reader)
            {
                using var readConn = TursoRawConnection.Open(reader);
                readConn.QueryScalar($"SELECT count(*) FROM {table}").Should().Be((long)rows);
                readConn.QueryScalar($"SELECT sum(id) FROM {table}").Should().Be((long)rows * (rows + 1) / 2);
                readConn.QueryScalar($"SELECT payload FROM {table} WHERE id = {rows}").Should().Be(payload);
            }
        }
        finally
        {
            DropRemoteTable(url, token, table);
            Cleanup(writerPath);
            Cleanup(readerPath);
        }
    }

    [TestMethod]
    public void RemoteSync_ServerError_SurfacesAsException()
    {
        if (!TursoNativeLibrary.IsAvailable())
        {
            Assert.Inconclusive("turso_sync_sdk_kit native library not found");
            return;
        }

        // A remote that answers 5xx during bootstrap must fail loudly, not hang or hand back an empty database.
        // Uses a local stub so it needs no cloud credentials — only the native. Guards the HTTP IO handler's
        // error propagation (the engine surfaces the non-2xx status as a TursoException out of Create).
        using var server = StubHttpServer.Start(HttpStatusCode.InternalServerError);
        var dbPath = TempDb();
        try
        {
            var act = () => TursoSyncDatabase.Create(new TursoSyncConfig
            {
                Path = dbPath,
                RemoteUrl = server.BaseUrl,
                BootstrapIfEmpty = true,
            });
            // The stub's own status, not merely some failure: the request must reach it and its 500 must be what
            // surfaces (on Windows an http.sys-based stub once answered with its own 400, and this still passed).
            act.Should().Throw<TursoException>().WithMessage("*status=500*");
            server.Requests.Should().BePositive();
        }
        finally
        {
            Cleanup(dbPath);
        }
    }

    [TestMethod]
    public void RemoteSync_ConcurrentWriters_DifferentKeys_BothMerge()
    {
        var (url, token) = RemoteOrSkip();

        // Two independent clients writing different rows must both land after a pull-rebase — neither clobbers
        // the other. Models the everyday multi-writer case (distinct from a same-key conflict).
        var table = "sync_cc_" + Guid.NewGuid().ToString("n");
        var seedPath = TempDb();
        var aPath = TempDb();
        var bPath = TempDb();
        var readerPath = TempDb();
        try
        {
            using (var seed = TursoSyncDatabase.Create(RemoteConfig(seedPath, url, token)))
            {
                using (var conn = TursoRawConnection.Open(seed))
                {
                    conn.Execute($"CREATE TABLE {table} (k INTEGER PRIMARY KEY, v TEXT)");
                    conn.Execute($"INSERT INTO {table} (k, v) VALUES (0, 'seed')");
                }

                seed.Push();
            }

            using (var a = TursoSyncDatabase.Create(RemoteConfig(aPath, url, token)))
            using (var b = TursoSyncDatabase.Create(RemoteConfig(bPath, url, token)))
            {
                using (var connA = TursoRawConnection.Open(a))
                {
                    connA.Execute($"INSERT INTO {table} (k, v) VALUES (1, 'from-a')");
                }

                a.Push();

                using (var connB = TursoRawConnection.Open(b))
                {
                    connB.Execute($"INSERT INTO {table} (k, v) VALUES (2, 'from-b')");
                }

                b.Pull();   // rebase onto A's committed change
                b.Push();
            }

            using var reader = TursoSyncDatabase.Create(RemoteConfig(readerPath, url, token));
            reader.Pull();
            using var readConn = TursoRawConnection.Open(reader);
            readConn.QueryScalar($"SELECT v FROM {table} WHERE k = 1").Should().Be("from-a");
            readConn.QueryScalar($"SELECT v FROM {table} WHERE k = 2").Should().Be("from-b");
            readConn.QueryScalar($"SELECT count(*) FROM {table}").Should().Be(3L);
        }
        finally
        {
            DropRemoteTable(url, token, table);
            Cleanup(seedPath);
            Cleanup(aPath);
            Cleanup(bPath);
            Cleanup(readerPath);
        }
    }

    [TestMethod]
    public void RemoteSync_ConcurrentWriters_SameKey_ConvergesToOneRow()
    {
        var (url, token) = RemoteOrSkip();

        // Two clients writing the SAME new key then pushing must converge to a single, consistent row — the
        // late push may fail with a conflict, but the remote must never end up corrupt or with a duplicate PK.
        // Which value wins is engine-defined, so assert only the invariant (one row, value is one of the two).
        var table = "sync_conf_" + Guid.NewGuid().ToString("n");
        var seedPath = TempDb();
        var aPath = TempDb();
        var bPath = TempDb();
        var readerPath = TempDb();
        try
        {
            using (var seed = TursoSyncDatabase.Create(RemoteConfig(seedPath, url, token)))
            {
                using (var conn = TursoRawConnection.Open(seed))
                {
                    conn.Execute($"CREATE TABLE {table} (k INTEGER PRIMARY KEY, v TEXT)");
                }

                seed.Push();
            }

            using (var a = TursoSyncDatabase.Create(RemoteConfig(aPath, url, token)))
            using (var b = TursoSyncDatabase.Create(RemoteConfig(bPath, url, token)))
            {
                using (var connA = TursoRawConnection.Open(a))
                {
                    connA.Execute($"INSERT INTO {table} (k, v) VALUES (1, 'from-a')");
                }

                using (var connB = TursoRawConnection.Open(b))
                {
                    connB.Execute($"INSERT INTO {table} (k, v) VALUES (1, 'from-b')");
                }

                a.Push();

                // B is now behind and holds a conflicting local insert; a pull-rebase or push may throw. Either
                // outcome is acceptable so long as the remote stays consistent (checked below).
                try
                {
                    b.Pull();
                    b.Push();
                }
                catch (TursoException)
                {
                    // conflict surfaced cleanly — acceptable
                }
            }

            using var reader = TursoSyncDatabase.Create(RemoteConfig(readerPath, url, token));
            reader.Pull();
            using var readConn = TursoRawConnection.Open(reader);
            readConn.QueryScalar($"SELECT count(*) FROM {table} WHERE k = 1").Should().Be(1L);
            readConn.QueryScalar($"SELECT v FROM {table} WHERE k = 1").Should().BeOfType<string>()
                .Which.Should().BeOneOf("from-a", "from-b");
        }
        finally
        {
            DropRemoteTable(url, token, table);
            Cleanup(seedPath);
            Cleanup(aPath);
            Cleanup(bPath);
            Cleanup(readerPath);
        }
    }

    [TestMethod]
    public void RemoteSync_Checkpoint_AgainstCloud()
    {
        var (url, token) = RemoteOrSkip();

        // Parity with the self-hosted checkpoint test, but end-to-end against Turso Cloud: fill the WAL,
        // checkpoint it away, then push and confirm the checkpointed data reached the remote.
        var table = "sync_ckpt_" + Guid.NewGuid().ToString("n");
        var dbPath = TempDb();
        try
        {
            using (var db = TursoSyncDatabase.Create(RemoteConfig(dbPath, url, token)))
            {
                using (var conn = TursoRawConnection.Open(db))
                {
                    conn.Execute($"CREATE TABLE {table} (x INTEGER)");
                    for (var i = 0; i < 1024; i++)
                    {
                        conn.Execute($"INSERT INTO {table} VALUES ({i})");
                    }
                }

                db.Checkpoint();
                db.Stats().MainWalSize.Should().Be(0);
                db.Push();
            }

            using var reader = TursoSyncDatabase.Create(RemoteConfig(TempDb(), url, token));
            reader.Pull();
            using var readConn = TursoRawConnection.Open(reader);
            readConn.QueryScalar($"SELECT sum(x) FROM {table}").Should().Be(1024L * 1023L / 2L);
        }
        finally
        {
            DropRemoteTable(url, token, table);
            Cleanup(dbPath);
        }
    }

    // ---- remote-sync harness -----------------------------------------------------------------------

    /// <summary>Skip (Inconclusive) unless the native and the cloud sync credentials are both present.</summary>
    private static (string Url, string? Token) RemoteOrSkip()
    {
        if (!TursoNativeLibrary.IsAvailable())
        {
            Assert.Inconclusive("turso_sync_sdk_kit native library not found");
        }

        var url = Environment.GetEnvironmentVariable("TURSOSYNC_TEST_REMOTE_URL");
        var token = Environment.GetEnvironmentVariable("TURSOSYNC_TEST_REMOTE_TOKEN");
        if (string.IsNullOrWhiteSpace(url))
        {
            Assert.Inconclusive("Set TURSOSYNC_TEST_REMOTE_URL (+ TURSOSYNC_TEST_REMOTE_TOKEN) to run the remote sync round-trip.");
        }

        return (url!, token);
    }

    private static TursoSyncConfig RemoteConfig(string path, string url, string? token) =>
        new() { Path = path, RemoteUrl = url, AuthToken = token, BootstrapIfEmpty = true };

    /// <summary>Drop a test table on the remote and push, so gated cloud tests don't accumulate schema.</summary>
    private static void DropRemoteTable(string url, string? token, string table)
    {
        try
        {
            var path = TempDb();
            using var db = TursoSyncDatabase.Create(RemoteConfig(path, url, token));
            using (var conn = TursoRawConnection.Open(db))
            {
                conn.Execute($"DROP TABLE IF EXISTS {table}");
            }

            db.Push();
            Cleanup(path);
        }
        catch
        {
            // best-effort remote cleanup — never fail a test on teardown
        }
    }

    private static string TempDb()
    {
        var dir = Path.Combine(Path.GetTempPath(), "tursosync-sync-" + Guid.NewGuid().ToString("n"));
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
    /// A minimal loopback HTTP server that answers every request with a fixed status code, for exercising the
    /// sync engine's error-propagation path without any real remote. Raw sockets, not <see cref="HttpListener"/>:
    /// on Windows that is http.sys, which answers the engine's <c>/pull-updates</c> with a 400 of its own (the
    /// engine sends <c>Accept-Encoding: application/protobuf</c>), so the configured status never reached the
    /// engine there. One request per connection, answered with <c>Connection: close</c>.
    /// </summary>
    private sealed class StubHttpServer : IDisposable
    {
        private readonly TcpListener _listener;
        private readonly HttpStatusCode _status;
        private int _requests;

        private StubHttpServer(TcpListener listener, HttpStatusCode status)
        {
            _listener = listener;
            _status = status;
            // 127.0.0.1, not localhost: the listener is IPv4-only, and a client trying ::1 first waits out a
            // refused connect on Windows before falling back.
            BaseUrl = $"http://127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}";
        }

        public string BaseUrl { get; }

        /// <summary>How many complete requests the stub has answered.</summary>
        public int Requests => Volatile.Read(ref _requests);

        public static StubHttpServer Start(HttpStatusCode status)
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var server = new StubHttpServer(listener, status);
            _ = server.LoopAsync();
            return server;
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
                    return; // listener stopped
                }

                _ = ServeAsync(client);
            }
        }

        private async Task ServeAsync(TcpClient client)
        {
            using (client)
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
                            return; // closed before a full request
                        }

                        head.Add(one[0]);
                    }

                    var length = System.Text.Encoding.ASCII.GetString(head.ToArray()).Split("\r\n")
                        .Select(l => l.Split(':', 2))
                        .Where(kv => kv.Length == 2 && kv[0].Trim().Equals("Content-Length", StringComparison.OrdinalIgnoreCase))
                        .Select(kv => int.Parse(kv[1].Trim()))
                        .FirstOrDefault();
                    await stream.ReadExactlyAsync(new byte[length]);

                    Interlocked.Increment(ref _requests);
                    await stream.WriteAsync(System.Text.Encoding.ASCII.GetBytes(
                        $"HTTP/1.1 {(int)_status} {_status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"));
                }
                catch
                {
                    // best-effort — the client may have already given up
                }
            }
        }

        public void Dispose() => _listener.Stop();
    }
}
