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
    public void Create_Rejects_Null_Config_Before_Reading_The_Threshold()
    {
        Action create = () => TursoSyncDatabase.Create(null!);
        create.Should().ThrowExactly<ArgumentNullException>().Which.ParamName.Should().Be("config");
    }

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
    [DataRow(false)]
    [DataRow(true)]
    public async Task Connections_Reject_Conflicting_Thresholds_Until_The_Shared_Engine_Is_Released(bool pooling)
    {
        if (!TursoNativeLibrary.IsAvailable()) Assert.Inconclusive("turso_sync_sdk_kit native library not found");
        var dir = Directory.CreateTempSubdirectory("tursosync-batching-");
        try
        {
            await using var server = new PushRemote();
            var options = new TursoConnectionStringBuilder
            {
                DataSource = Path.Combine(dir.FullName, "store.db"), RemoteUrl = server.Url,
                Pooling = pooling, SchemaGuard = TursoSchemaGuardMode.Off,
            };
            var defaultThreshold = options.ConnectionString;
            options.PushOperationsThreshold = 1000;
            var explicitDefault = options.ConnectionString;
            options.PushOperationsThreshold = 0;
            using var conflicting = new TursoConnection(options.ConnectionString);
            Action openConflicting = conflicting.Open;
            using (var first = new TursoConnection(defaultThreshold))
            using (var compatible = new TursoConnection(explicitDefault))
            {
                first.Open();
                compatible.Open();
                compatible.SyncDatabase.Should().BeSameAs(first.SyncDatabase);
                openConflicting.Should().Throw<InvalidOperationException>().WithMessage("*Push Operations Threshold*");
                conflicting.State.Should().Be(System.Data.ConnectionState.Closed);
                TursoSyncDatabaseCache.RefCountFor(options.DataSource).Should().Be(2);
            }

            if (pooling)
            {
                openConflicting.Should().Throw<InvalidOperationException>().WithMessage("*Push Operations Threshold*");
                TursoSyncDatabaseCache.RefCountFor(options.DataSource).Should().Be(2);
            }
            TursoConnection.ClearPool();
            TursoSyncDatabaseCache.RefCountFor(options.DataSource).Should().Be(0);

            conflicting.Open();
            conflicting.Raw.Execute("CREATE TABLE items (value TEXT)");
            for (var i = 0; i < 5; i++) conflicting.Raw.Execute($"INSERT INTO items VALUES ('batch-row-{i}')");
            conflicting.SyncDatabase!.Push();
            server.Batches.Select(b => b.Length).Should().Equal(5);
        }
        finally
        {
            TursoConnection.ClearPool();
            dir.Delete(recursive: true);
        }
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

    [TestMethod]
    [DataRow(0)]
    [DataRow(1)]
    [DataRow(2)]
    public async Task A_Statement_Constraint_Failure_Rolls_Back_The_Batch_And_Its_Watermark(int failedRow)
    {
        await WithReplica(0, (db, conn, server) =>
        {
            for (var i = 0; i < 3; i++) conn.Execute($"INSERT INTO items VALUES ('batch-row-{i}')");
            var acknowledged = server.Watermark;
            server.FailStatementRow = $"batch-row-{failedRow}";
            Action push = db.Push;
            var error = push.Should().Throw<TursoException>().Which;
            server.Batches.Should().BeEmpty("a statement error must roll back all rows in this batch");
            server.Watermark.Should().Be(acknowledged, "a failed batch must remain retryable");
            error.Message.Should().Contain("table=items").And.Contain("change_id=");

            server.FailStatementRow = null;
            db.Push();
            server.Batches.SelectMany(b => b).Should().Equal("batch-row-0", "batch-row-1", "batch-row-2");
        });
    }

    [TestMethod]
    public async Task A_Statement_Failure_Preserves_Previously_Acknowledged_Batches()
    {
        await WithReplica(2, (db, conn, server) =>
        {
            for (var i = 0; i < 5; i++) conn.Execute($"INSERT INTO items VALUES ('batch-row-{i}')");
            server.FailStatementRow = "batch-row-3";
            Action push = db.Push;
            push.Should().Throw<TursoException>();
            server.Batches.SelectMany(b => b).Should().Equal("batch-row-0", "batch-row-1");
            server.FailStatementRow = null;
            db.Push();
            server.Batches.SelectMany(b => b).Should().Equal(Enumerable.Range(0, 5).Select(i => $"batch-row-{i}"));
        });
    }

    [TestMethod]
    public async Task A_Commit_Failure_Rolls_Back_The_Remote_Transaction_And_Preserves_The_Watermark()
    {
        await WithReplica(0, (db, conn, server) =>
        {
            conn.Execute("INSERT INTO items VALUES ('batch-row-0')");
            var acknowledged = server.Watermark;
            server.FailCommit = true;
            Action push = db.Push;
            push.Should().Throw<TursoException>();
            server.Batches.Should().BeEmpty();
            server.Watermark.Should().Be(acknowledged);
            server.LastBatchLeftTransactionOpen.Should().BeFalse("a failed COMMIT must release the remote write transaction");
            server.FailCommit = false;
            db.Push();
            server.Batches.SelectMany(b => b).Should().Equal("batch-row-0");
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
        public string? FailStatementRow { get; set; }
        public bool FailCommit { get; set; }
        public bool LastBatchLeftTransactionOpen { get; private set; }
        public string? Watermark => _watermark is null ? null : JsonSerializer.Serialize(_watermark);

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
                    var rows = steps.EnumerateArray().SelectMany(s => s.GetProperty("stmt").GetProperty("args").EnumerateArray())
                        .Where(a => a.GetProperty("type").GetString() == "text")
                        .Select(a => a.GetProperty("value").GetString()!)
                        .Where(v => v.StartsWith("batch-row-", StringComparison.Ordinal)).ToArray();
                    var fail = rows.Length > 0 && FailBatch == Batches.Count + 1;
                    var result = fail ? null : ExecuteBatch(steps);
                    var responseBody = fail ? "rejected batch" : JsonSerializer.Serialize(new
                    {
                        baton = (string?)null, base_url = (string?)null,
                        results = new[] { new { type = "ok", response = new { type = "batch", result } } },
                    });
                    var payload = Encoding.UTF8.GetBytes(responseBody);
                    var response = Encoding.ASCII.GetBytes($"HTTP/1.1 {(fail ? "500 Internal Server Error" : "200 OK")}\r\nContent-Type: application/json\r\nContent-Length: {payload.Length}\r\nConnection: close\r\n\r\n");
                    await stream.WriteAsync(response, _stop.Token);
                    await stream.WriteAsync(payload, _stop.Token);
                }
            }
            catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
        }

        // Hrana conditions execute on the server. Model transaction state and per-statement errors rather
        // than turning a SQL constraint failure into an HTTP failure (which never exercises COMMIT gating).
        private object ExecuteBatch(JsonElement steps)
        {
            var results = new object?[steps.GetArrayLength()];
            var errors = new object?[steps.GetArrayLength()];
            var inTransaction = false;
            var pendingRows = new List<string>();
            var pendingWatermark = _watermark;
            bool Allows(JsonElement condition) => condition.GetProperty("type").GetString() switch
            {
                "ok" => results[condition.GetProperty("step").GetInt32()] is not null,
                "error" => errors[condition.GetProperty("step").GetInt32()] is not null,
                "not" => !Allows(condition.GetProperty("cond")),
                "and" => condition.GetProperty("conds").EnumerateArray().All(Allows),
                "or" => condition.GetProperty("conds").EnumerateArray().Any(Allows),
                "is_autocommit" => !inTransaction,
                _ => throw new InvalidOperationException("Unexpected Hrana condition"),
            };
            var index = 0;
            foreach (var step in steps.EnumerateArray())
            {
                var current = index++;
                if (step.TryGetProperty("condition", out var condition) && condition.ValueKind != JsonValueKind.Null && !Allows(condition)) continue;
                var statement = step.GetProperty("stmt");
                var sql = statement.GetProperty("sql").GetString()!;
                var args = statement.GetProperty("args").EnumerateArray().ToArray();
                var rows = args.Where(a => a.GetProperty("type").GetString() == "text")
                    .Select(a => a.GetProperty("value").GetString()!)
                    .Where(v => v.StartsWith("batch-row-", StringComparison.Ordinal)).ToArray();
                if ((FailStatementRow is not null && rows.Contains(FailStatementRow)) || (FailCommit && sql == "COMMIT"))
                {
                    errors[current] = new { message = "SQLite error: FOREIGN KEY constraint failed", code = "SQLITE_CONSTRAINT" };
                    continue;
                }
                if (sql.StartsWith("BEGIN", StringComparison.Ordinal)) inTransaction = true;
                else if (sql == "ROLLBACK") { inTransaction = false; pendingRows.Clear(); pendingWatermark = _watermark; }
                else if (sql == "COMMIT")
                {
                    inTransaction = false;
                    _watermark = pendingWatermark;
                    if (pendingRows.Count > 0) Batches.Add(pendingRows.ToArray());
                }
                else if (sql.Contains("INSERT INTO turso_sync_last_change_id", StringComparison.OrdinalIgnoreCase))
                    pendingWatermark = args.Skip(1).Select(a => a.Clone()).ToArray();
                else pendingRows.AddRange(rows);
                object[] values = sql.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase) && _watermark is not null ? [_watermark] : [];
                results[current] = new { cols = Array.Empty<object>(), rows = values, affected_row_count = 0, last_insert_rowid = (string?)null };
            }
            LastBatchLeftTransactionOpen = inTransaction;
            return new { step_results = results, step_errors = errors };
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
