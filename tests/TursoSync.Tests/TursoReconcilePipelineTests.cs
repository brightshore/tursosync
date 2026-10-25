using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Turso.Sync;

namespace TursoSync.Tests;

/// <summary>
/// <see cref="TursoSyncDatabase.ReconcileLocalTables"/> learns the server's tables with one SQL request over
/// <c>/v2/pipeline</c>, not by bootstrapping a throwaway replica, which downloads the whole database to read a few
/// names. The bootstrap is kept only as the fallback when the server cannot answer that way. All offline, against a
/// loopback stub that speaks just enough Hrana.
/// </summary>
[TestClass]
public class TursoReconcilePipelineTests
{
    [TestMethod]
    public void The_server_tables_come_from_one_pipeline_select_without_internal_names()
    {
        using var server = PipelineStub.Start(_ => PipelineStub.Rows("lane", "sqlite_sequence", "turso_cdc", "__turso_internal", "project"));
        using var http = new HttpClient();

        var tables = TursoSyncDatabase.ServerTablesOverPipeline(http, server.BaseUrl, "tok", null, out var failure);

        tables.Should().Equal("lane", "project");
        failure.Should().BeNull();
        server.Requests.Should().ContainSingle();
        var request = server.Requests.Single();
        request.Path.Should().Be("/v2/pipeline");
        request.Authorization.Should().Be("Bearer tok");
        using var sent = System.Text.Json.JsonDocument.Parse(request.Body);
        sent.RootElement.GetProperty("requests")[0].GetProperty("stmt").GetProperty("sql").GetString()
            .Should().Contain("SELECT name FROM sqlite_master WHERE type = 'table'");
    }

    [TestMethod]
    public void A_server_that_answers_with_an_error_status_gives_no_list()
    {
        using var server = PipelineStub.Start(_ => (HttpStatusCode.InternalServerError, "boom"));
        using var http = new HttpClient();

        TursoSyncDatabase.ServerTablesOverPipeline(http, server.BaseUrl, null, null, out var failure).Should().BeNull();
        failure.Should().Contain("500");
    }

    [TestMethod]
    public void A_failed_statement_gives_no_list()
    {
        using var server = PipelineStub.Start(_ => (HttpStatusCode.OK,
            """{"baton":null,"base_url":null,"results":[{"type":"error","error":{"message":"no","code":"SQLITE_ERROR"}}]}"""));
        using var http = new HttpClient();

        TursoSyncDatabase.ServerTablesOverPipeline(http, server.BaseUrl, null, null, out var failure).Should().BeNull();
        failure.Should().Contain("failed");
    }

    [TestMethod]
    public void A_response_that_is_not_hrana_gives_no_list()
    {
        using var server = PipelineStub.Start(_ => (HttpStatusCode.OK, """{"results":[{"type":"ok"}]}"""));
        using var http = new HttpClient();

        TursoSyncDatabase.ServerTablesOverPipeline(http, server.BaseUrl, null, null, out var failure).Should().BeNull();
        failure.Should().Contain("KeyNotFoundException");
    }

    [TestMethod]
    public void A_server_that_knows_every_table_is_reconciled_without_a_bootstrap()
    {
        if (!TursoNativeLibrary.IsAvailable())
        {
            Assert.Inconclusive("turso_sync_sdk_kit native library not found");
        }

        // The stub answers the pipeline with the local tables, and refuses everything else, so a bootstrap of a
        // scratch replica (the old way to read the server's tables) would surface as a non-pipeline request.
        using var server = PipelineStub.Start(path =>
            path == "/v2/pipeline" ? PipelineStub.Rows("notes") : (HttpStatusCode.InternalServerError, "not a sync server"));
        var dir = Path.Combine(Path.GetTempPath(), "tursosync-reconcile-pipe-" + Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(dir);
        try
        {
            using var db = TursoSyncDatabase.Create(new TursoSyncConfig
            {
                Path = Path.Combine(dir, "store.db"),
                RemoteUrl = server.BaseUrl,
                SchemaGuard = TursoSchemaGuardMode.Off,
            });
            using (var conn = TursoRawConnection.Open(db))
            {
                conn.Execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)");
            }

            server.Requests.Clear();
            var result = db.ReconcileLocalTables(new TursoReconcileOptions { PushAfter = false });

            result.RebuiltTables.Should().BeEmpty();
            result.PipelineFallbackReason.Should().BeNull();
            server.Requests.Select(r => r.Path).Should().Equal("/v2/pipeline");
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch (IOException) { /* best-effort teardown */ }
        }
    }

    [TestMethod]
    public void A_server_that_cannot_answer_the_pipeline_falls_back_to_the_bootstrap()
    {
        if (!TursoNativeLibrary.IsAvailable())
        {
            Assert.Inconclusive("turso_sync_sdk_kit native library not found");
        }

        // Every request fails, so the pipeline gives no list and the reconcile reaches for the scratch bootstrap:
        // it is the bootstrap's request (any non-pipeline path) that proves the fallback ran, and its failure that
        // surfaces as the TursoException, as before this change.
        using var server = PipelineStub.Start(_ => (HttpStatusCode.InternalServerError, "down"));
        var dir = Path.Combine(Path.GetTempPath(), "tursosync-reconcile-pipe-" + Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(dir);
        try
        {
            using var db = TursoSyncDatabase.Create(new TursoSyncConfig
            {
                Path = Path.Combine(dir, "store.db"),
                RemoteUrl = server.BaseUrl,
                SchemaGuard = TursoSchemaGuardMode.Off,
            });

            server.Requests.Clear();
            var act = () => db.ReconcileLocalTables(new TursoReconcileOptions
            {
                PushAfter = false,
                ScratchDirectory = Path.Combine(dir, "scratch"),
            });

            var thrown = act.Should().Throw<TursoException>().Which;
            server.Requests.Select(r => r.Path).Should().Contain("/v2/pipeline")
                .And.Contain(p => p != "/v2/pipeline",
                    "the bootstrap's request must reach the stub (the reconcile threw: {0}; the stub saw: {1})",
                    thrown.Message, string.Join("; ", server.Requests.Select(r => $"{r.Path} {r.Body}")));
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch (IOException) { /* best-effort teardown */ }
        }
    }

    /// <summary>
    /// A loopback HTTP server that records each request and answers it from a handler keyed on the path. Raw
    /// sockets, not <see cref="HttpListener"/>: on Windows that is http.sys, which rejects the sync engine's own
    /// <c>/pull-updates</c> request with a 400 of its own before the handler sees it (the engine sends
    /// <c>Accept-Encoding: application/protobuf</c>, a media type where a content coding belongs), so a stub on
    /// it can never observe the bootstrap there. One request per connection, answered with <c>Connection: close</c>.
    /// </summary>
    private sealed class PipelineStub : IDisposable
    {
        private readonly TcpListener _listener;
        private readonly Func<string, (HttpStatusCode Status, string Body)> _respond;

        private PipelineStub(TcpListener listener, Func<string, (HttpStatusCode, string)> respond)
        {
            _listener = listener;
            _respond = respond;
            // 127.0.0.1, not localhost: the listener is IPv4-only, and a client trying ::1 first waits out a
            // refused connect on Windows before falling back.
            BaseUrl = $"http://127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}";
        }

        public string BaseUrl { get; }

        public ConcurrentQueue<Recorded> Requests { get; } = new();

        public sealed record Recorded(string Path, string? Authorization, string Body);

        /// <summary>A Hrana <c>execute</c> result with one text column holding each of <paramref name="names"/>.</summary>
        public static (HttpStatusCode, string) Rows(params string[] names)
        {
            var rows = string.Join(",", names.Select(n => $$"""[{"type":"text","value":"{{n}}"}]"""));
            return (HttpStatusCode.OK,
                """{"baton":null,"base_url":null,"results":[{"type":"ok","response":{"type":"execute","result":{"cols":[{"name":"name","decltype":"TEXT"}],"rows":["""
                + rows
                + """],"affected_row_count":0,"last_insert_rowid":null}}}]}""");
        }

        public static PipelineStub Start(Func<string, (HttpStatusCode, string)> respond)
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var server = new PipelineStub(listener, respond);
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
                catch (Exception ex) when (ex is SocketException or ObjectDisposedException or InvalidOperationException)
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
                    var head = await ReadHeadAsync(stream);
                    if (head is null)
                    {
                        return; // closed before a full request
                    }

                    var lines = head.Split("\r\n");
                    var target = lines[0].Split(' ')[1];
                    var path = target.Split('?')[0];
                    var headers = lines.Skip(1)
                        .Select(l => l.Split(':', 2))
                        .Where(kv => kv.Length == 2)
                        .ToDictionary(kv => kv[0].Trim(), kv => kv[1].Trim(), StringComparer.OrdinalIgnoreCase);
                    var length = headers.TryGetValue("Content-Length", out var cl) ? int.Parse(cl) : 0;
                    var body = new byte[length];
                    await stream.ReadExactlyAsync(body);

                    Requests.Enqueue(new Recorded(path, headers.GetValueOrDefault("Authorization"), Encoding.UTF8.GetString(body)));
                    var (status, text) = _respond(path);
                    var payload = Encoding.UTF8.GetBytes(text);
                    var response = Encoding.ASCII.GetBytes(
                        $"HTTP/1.1 {(int)status} {status}\r\nContent-Type: application/json\r\n" +
                        $"Content-Length: {payload.Length}\r\nConnection: close\r\n\r\n");
                    await stream.WriteAsync(response);
                    await stream.WriteAsync(payload);
                }
                catch (Exception ex) when (ex is IOException or SocketException or ObjectDisposedException)
                {
                    // the client gave up on this request; the accept loop keeps serving the next one
                }
            }
        }

        /// <summary>The request line and headers, up to the blank line; null if the client closed first.</summary>
        private static async Task<string?> ReadHeadAsync(NetworkStream stream)
        {
            var head = new List<byte>();
            var one = new byte[1];
            while (head.Count < 4 || head[^4] != '\r' || head[^3] != '\n' || head[^2] != '\r' || head[^1] != '\n')
            {
                if (await stream.ReadAsync(one) == 0)
                {
                    return null;
                }

                head.Add(one[0]);
            }

            return Encoding.ASCII.GetString(head.ToArray(), 0, head.Count - 4);
        }

        public void Dispose() => _listener.Stop();
    }
}
