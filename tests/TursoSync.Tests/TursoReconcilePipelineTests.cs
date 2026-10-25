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

    /// <summary>A loopback HTTP server that records each request and answers it from a handler keyed on the path.</summary>
    private sealed class PipelineStub : IDisposable
    {
        private readonly HttpListener _listener;
        private readonly Func<string, (HttpStatusCode Status, string Body)> _respond;

        private PipelineStub(HttpListener listener, int port, Func<string, (HttpStatusCode, string)> respond)
        {
            _listener = listener;
            _respond = respond;
            BaseUrl = $"http://localhost:{port}";
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
            var port = FreePort();
            var listener = new HttpListener();
            listener.Prefixes.Add($"http://localhost:{port}/");
            listener.Start();
            var server = new PipelineStub(listener, port, respond);
            _ = server.LoopAsync();
            return server;
        }

        private async Task LoopAsync()
        {
            while (_listener.IsListening)
            {
                HttpListenerContext ctx;
                try
                {
                    ctx = await _listener.GetContextAsync();
                }
                catch (Exception ex) when (ex is HttpListenerException or ObjectDisposedException or InvalidOperationException)
                {
                    return; // listener stopped
                }

                try
                {
                    using var reader = new StreamReader(ctx.Request.InputStream, Encoding.UTF8);
                    var body = await reader.ReadToEndAsync();
                    var path = ctx.Request.Url?.AbsolutePath ?? "";
                    Requests.Enqueue(new Recorded(path, ctx.Request.Headers["Authorization"], body));
                    var (status, text) = _respond(path);
                    var bytes = Encoding.UTF8.GetBytes(text);
                    ctx.Response.StatusCode = (int)status;
                    ctx.Response.ContentType = "application/json";
                    await ctx.Response.OutputStream.WriteAsync(bytes);
                    ctx.Response.Close();
                }
                catch (Exception ex) when (ex is HttpListenerException or IOException or ObjectDisposedException)
                {
                    // the client gave up on this request; keep serving the next one
                    Requests.Enqueue(new Recorded(ctx.Request.Url?.AbsolutePath ?? "", null, "STUB-ERROR " + ex.GetType().Name + ": " + ex.Message));
                }
            }
        }

        public void Dispose()
        {
            try
            {
                _listener.Stop();
                _listener.Close();
            }
            catch (ObjectDisposedException)
            {
                // already closed
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
    }
}
