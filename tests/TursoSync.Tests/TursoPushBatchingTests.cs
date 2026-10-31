using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Turso.Sync;

namespace TursoSync.Tests;

[TestClass]
public class TursoPushBatchingTests
{
    [TestMethod]
    public void ConnectionString_Propagates_The_Threshold_And_Defaults_To_Bounded_Pushes()
    {
        var defaults = new TursoConnectionStringBuilder("Data Source=unused");
        defaults.ToConfig().PushOperationsThreshold.Should().Be(1000);
        var configured = new TursoConnectionStringBuilder("Data Source=unused;PushOperationsThreshold=2");
        configured.PushOperationsThreshold.Should().Be(2);
        configured.ToConfig().PushOperationsThreshold.Should().Be(2);
        configured.ConnectionString.Should().Contain("Push Operations Threshold");
        configured.PoolKey.Should().NotBe(defaults.PoolKey);

        Action invalid = () => TursoSyncDatabase.Create(new TursoSyncConfig { Path = "unused", PushOperationsThreshold = -1 });
        invalid.Should().Throw<ArgumentOutOfRangeException>();
    }

    [TestMethod]
    [DataRow(0, 1)]
    [DataRow(2, 3)]
    public async Task Push_Splits_The_Actual_Native_Requests_At_The_Configured_Threshold(int threshold, int batches)
    {
        await WithReplica(threshold, (db, conn, server) =>
        {
            for (var i = 0; i < 5; i++) conn.Execute($"INSERT INTO items VALUES ('batch-row-{i}')");
            db.Push();
            server.Batches.Should().HaveCount(batches);
            server.Batches.SelectMany(b => b).Should().Equal(Enumerable.Range(0, 5).Select(i => $"batch-row-{i}"));
            if (threshold > 0) server.Batches.Should().OnlyContain(b => b.Length <= threshold);
        });
    }

    [TestMethod]
    public async Task Default_Threshold_Drains_A_Backlog_In_Multiple_Requests()
    {
        await WithReplica(null, (db, conn, server) =>
        {
            for (var i = 0; i < 1001; i++) conn.Execute($"INSERT INTO items VALUES ('batch-row-{i}')");
            db.Push();
            server.Batches.Select(b => b.Length).Should().Equal(1000, 1);
        });
    }

    [TestMethod]
    public async Task A_Transaction_Larger_Than_The_Threshold_Is_Never_Split()
    {
        await WithReplica(2, (db, conn, server) =>
        {
            conn.Execute("BEGIN");
            for (var i = 0; i < 3; i++) conn.Execute($"INSERT INTO items VALUES ('batch-row-{i}')");
            conn.Execute("COMMIT");
            conn.Execute("INSERT INTO items VALUES ('batch-row-3')");
            db.Push();
            server.Batches.Select(b => b.Length).Should().Equal(3, 1);
        });
    }

    [TestMethod]
    public async Task Retrying_A_Failed_Push_Resumes_After_The_Last_Acknowledged_Batch()
    {
        await WithReplica(2, (db, conn, server) =>
        {
            for (var i = 0; i < 5; i++) conn.Execute($"INSERT INTO items VALUES ('batch-row-{i}')");
            server.FailBatch = 2;
            Action firstPush = db.Push;
            firstPush.Should().Throw<TursoException>();
            server.Batches.SelectMany(b => b).Should().Equal("batch-row-0", "batch-row-1");
            server.FailBatch = null;
            db.Push();
            server.Batches.SelectMany(b => b).Should().Equal(Enumerable.Range(0, 5).Select(i => $"batch-row-{i}"));
        });
    }

    private static async Task WithReplica(int? threshold, Action<TursoSyncDatabase, TursoRawConnection, PushRemote> test)
    {
        if (!TursoNativeLibrary.IsAvailable()) Assert.Inconclusive("turso_sync_sdk_kit native library not found");
        var dir = Directory.CreateTempSubdirectory("tursosync-batching-");
        try
        {
            await using var server = new PushRemote();
            var config = new TursoSyncConfig
            {
                Path = Path.Combine(dir.FullName, "store.db"), RemoteUrl = server.Url,
                BootstrapIfEmpty = false, SchemaGuard = TursoSchemaGuardMode.Off,
            };
            if (threshold is { } value) config = config with { PushOperationsThreshold = value };
            using var db = TursoSyncDatabase.Create(config);
            using var conn = TursoRawConnection.Open(db);
            conn.Execute("CREATE TABLE items (value TEXT)");
            db.Push(); // establish the schema watermark before the row batches under test
            test(db, conn, server);
        }
        finally { dir.Delete(recursive: true); }
    }

    // Only the Hrana push protocol is stubbed. The native engine records transactions, builds each HTTP
    // request and resumes from the watermark this remote acknowledges, exactly as it does against cloud.
    private sealed class PushRemote : IAsyncDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly CancellationTokenSource _stop = new();
        private readonly Task _serve;
        private JsonElement[]? _watermark;
        public string Url { get; }
        public List<string[]> Batches { get; } = [];
        public int? FailBatch { get; set; }

        public PushRemote()
        {
            _listener.Start();
            Url = $"http://127.0.0.1:{((IPEndPoint)_listener.LocalEndpoint).Port}";
            _serve = Serve();
        }

        private async Task Serve()
        {
            try
            {
                while (!_stop.IsCancellationRequested)
                {
                    using var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                    using var stream = client.GetStream();
                    var header = new List<byte>();
                    var one = new byte[1];
                    while (header.Count < 4 || !header.TakeLast(4).SequenceEqual("\r\n\r\n".Select(c => (byte)c)))
                    {
                        await stream.ReadExactlyAsync(one, _stop.Token);
                        header.Add(one[0]);
                    }
                    var length = Encoding.ASCII.GetString(header.ToArray()).Split("\r\n")
                        .Single(l => l.StartsWith("Content-Length:", StringComparison.OrdinalIgnoreCase)).Split(':')[1];
                    var body = new byte[int.Parse(length.Trim())];
                    await stream.ReadExactlyAsync(body, _stop.Token);
                    using var request = JsonDocument.Parse(body);
                    var steps = request.RootElement.GetProperty("requests")[0].GetProperty("batch").GetProperty("steps");
                    var select = steps.GetArrayLength() == 1;
                    var rows = steps.EnumerateArray().SelectMany(s => s.GetProperty("stmt").GetProperty("args").EnumerateArray())
                        .Where(a => a.GetProperty("type").GetString() == "text")
                        .Select(a => a.GetProperty("value").GetString()!)
                        .Where(v => v.StartsWith("batch-row-", StringComparison.Ordinal)).ToArray();
                    var fail = rows.Length > 0 && FailBatch == Batches.Count + 1;
                    if (!select && !fail)
                    {
                        if (rows.Length > 0) Batches.Add(rows);
                        var watermark = steps.EnumerateArray().Select(s => s.GetProperty("stmt"))
                            .Single(s => s.GetProperty("sql").GetString()!.Contains("INSERT INTO turso_sync_last_change_id", StringComparison.OrdinalIgnoreCase));
                        _watermark = watermark.GetProperty("args").EnumerateArray().Skip(1).Select(a => a.Clone()).ToArray();
                    }
                    object[] values = select && _watermark is not null ? [ _watermark ] : [];
                    var result = new { cols = Array.Empty<object>(), rows = values, affected_row_count = 0, last_insert_rowid = (string?)null };
                    var responseBody = fail ? "rejected batch" : JsonSerializer.Serialize(new
                    {
                        baton = (string?)null, base_url = (string?)null,
                        results = new[] { new { type = "ok", response = new { type = "batch", result = new
                        {
                            step_results = Enumerable.Repeat(result, steps.GetArrayLength()).ToArray(),
                            step_errors = new object?[steps.GetArrayLength()],
                        } } } },
                    });
                    var payload = Encoding.UTF8.GetBytes(responseBody);
                    var response = Encoding.ASCII.GetBytes($"HTTP/1.1 {(fail ? "500 Internal Server Error" : "200 OK")}\r\nContent-Type: application/json\r\nContent-Length: {payload.Length}\r\nConnection: close\r\n\r\n");
                    await stream.WriteAsync(response, _stop.Token);
                    await stream.WriteAsync(payload, _stop.Token);
                }
            }
            catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
        }

        public async ValueTask DisposeAsync()
        {
            await _stop.CancelAsync();
            _listener.Stop();
            await _serve;
            _stop.Dispose();
        }
    }
}
