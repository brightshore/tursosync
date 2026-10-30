using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Turso.Sync;

namespace TursoSync.Tests;

[TestClass]
public class TursoSyncHttpDiagnosticsTests
{
    [TestMethod]
    public async Task NormalCompletion_ReportsSafeMetadataAndPreservesResponse()
    {
        await using var server = new StubServer(async (stream, token) =>
        {
            await Write(stream, "HTTP/1.1 200 OK\r\nContent-Length: 5\r\nX-Secret: response-secret\r\nConnection: close\r\n\r\nhello", token);
        });
        var events = new List<TursoSyncHttpDiagnostic>();
        var result = Send(server, events.Add);

        result.Status.Should().Be(200);
        result.Body.Should().Be("hello");
        var diagnostic = events.Should().ContainSingle().Subject;
        diagnostic.Method.Should().Be("POST");
        diagnostic.Endpoint.Should().Be("push-updates");
        diagnostic.RequestBodyBytes.Should().Be(14);
        diagnostic.StatusCode.Should().Be(200);
        diagnostic.DeclaredResponseBytes.Should().Be(5);
        diagnostic.ActualResponseBytes.Should().Be(5);
        diagnostic.FailurePhase.Should().Be(TursoSyncHttpFailurePhase.None);
        diagnostic.TimeToResponseHeaders.Should().NotBeNull();
        diagnostic.TotalDuration.Should().BeGreaterThanOrEqualTo(diagnostic.TimeToResponseHeaders!.Value);
        var json = JsonSerializer.Serialize(diagnostic);
        foreach (var secret in new[] { server.Url, "query-secret", "token-secret", "header-secret", "response-secret", "payload-secret", "hello" })
        {
            json.Should().NotContain(secret);
        }
    }

    [TestMethod]
    public async Task DelayedHeaders_AreIncludedInHeaderTiming()
    {
        await using var server = new StubServer(async (stream, token) =>
        {
            await Task.Delay(150, token);
            await Write(stream, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n", token);
        });
        TursoSyncHttpDiagnostic? diagnostic = null;
        Send(server, value => diagnostic = value);
        diagnostic!.TimeToResponseHeaders!.Value.Should().BeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(100));
        diagnostic.ActualResponseBytes.Should().Be(0);
        diagnostic.FailurePhase.Should().Be(TursoSyncHttpFailurePhase.None);
    }

    [TestMethod]
    public async Task HeaderTimeout_ReportsBeforeHeadersAndPreservesException()
    {
        await using var server = new StubServer((_, token) => Task.Delay(Timeout.Infinite, token));
        var events = new List<TursoSyncHttpDiagnostic>();
        Action send = () => Send(server, events.Add, timeoutMs: 500);
        send.Should().Throw<TaskCanceledException>();
        var diagnostic = events.Should().ContainSingle().Subject;
        diagnostic.TimeToResponseHeaders.Should().BeNull();
        diagnostic.StatusCode.Should().BeNull();
        diagnostic.DeclaredResponseBytes.Should().BeNull();
        diagnostic.ActualResponseBytes.Should().BeNull();
        diagnostic.FailurePhase.Should().Be(TursoSyncHttpFailurePhase.BeforeHeaders);
        diagnostic.TotalDuration.Should().BeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(300));
    }

    [TestMethod]
    public async Task StalledBody_ReportsPartialBytesAndBodyFailure()
    {
        await using var server = new StubServer(async (stream, token) =>
        {
            await Write(stream, "HTTP/1.1 200 OK\r\nContent-Length: 10\r\nConnection: close\r\n\r\nabc", token);
            await Task.Delay(Timeout.Infinite, token);
        });
        var events = new List<TursoSyncHttpDiagnostic>();
        Action send = () => Send(server, events.Add, timeoutMs: 500);
        send.Should().Throw<TimeoutException>().WithMessage("the response body stopped arriving");
        var diagnostic = events.Should().ContainSingle().Subject;
        diagnostic.StatusCode.Should().Be(200);
        diagnostic.DeclaredResponseBytes.Should().Be(10);
        diagnostic.ActualResponseBytes.Should().Be(3);
        diagnostic.TimeToResponseHeaders.Should().NotBeNull();
        (diagnostic.TotalDuration - diagnostic.TimeToResponseHeaders!.Value).Should()
            .BeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(300));
        diagnostic.FailurePhase.Should().Be(TursoSyncHttpFailurePhase.ResponseBody);
    }

    [TestMethod]
    public async Task ThrowingCallback_DoesNotFailCompletion()
    {
        await using var server = new StubServer((stream, token) =>
            Write(stream, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok", token));
        var calls = 0;
        var result = Send(server, _ => { calls++; throw new InvalidOperationException("callback failed"); });
        result.Should().Be((200, "ok"));
        calls.Should().Be(1);
    }

    [TestMethod]
    public async Task ThrowingCallback_DoesNotReplaceBodyFailure()
    {
        await using var server = new StubServer(async (stream, token) =>
        {
            await Write(stream, "HTTP/1.1 200 OK\r\nContent-Length: 10\r\nConnection: close\r\n\r\nabc", token);
            await Task.Delay(Timeout.Infinite, token);
        });
        var calls = 0;
        Action send = () => Send(server, _ => { calls++; throw new InvalidOperationException(); }, timeoutMs: 500);
        send.Should().Throw<TimeoutException>().WithMessage("the response body stopped arriving");
        calls.Should().Be(1);
    }

    [TestMethod]
    public async Task UnknownEndpointAndMethod_AreNotEmitted_AndUndeclaredLengthIsNull()
    {
        await using var server = new StubServer((stream, token) =>
            Write(stream, "HTTP/1.1 503 Unavailable\r\nConnection: close\r\n\r\nerror", token));
        TursoSyncHttpDiagnostic? diagnostic = null;
        var result = Send(server, value => diagnostic = value, path: "/path-secret", method: "method-secret");
        result.Should().Be((503, "error"));
        diagnostic!.Method.Should().Be("OTHER");
        diagnostic.Endpoint.Should().Be("other");
        diagnostic.DeclaredResponseBytes.Should().BeNull();
        diagnostic.ActualResponseBytes.Should().Be(5);
        diagnostic.FailurePhase.Should().Be(TursoSyncHttpFailurePhase.None);
        JsonSerializer.Serialize(diagnostic).Should().NotContain("secret");
    }

    [TestMethod]
    public async Task Diagnostics_DefaultToOff_AndNullCallbackPreservesResponse()
    {
        new TursoSyncConfig { Path = "unused" }.HttpDiagnostics.Should().BeNull();
        await using var server = new StubServer((stream, token) =>
            Write(stream, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok", token));
        Send(server, null).Should().Be((200, "ok"));
    }

    private static (int Status, string Body) Send(StubServer server,
        Action<TursoSyncHttpDiagnostic>? callback, int timeoutMs = 5000,
        string path = "/push-updates", string method = "POST")
    {
        using var client = new HttpClient { Timeout = TimeSpan.FromMilliseconds(timeoutMs) };
        using var message = TursoSyncDatabase.BuildHttpMessage(method, server.Url + path + "?key=query-secret",
            Encoding.UTF8.GetBytes("payload-secret"),
            [new KeyValuePair<string, string>("X-Secret", "header-secret")], "token-secret", null);
        var status = 0;
        using var body = new MemoryStream();
        TursoSyncDatabase.SendHttp(client, message, callback, value => status = value, stream => stream.CopyTo(body));
        return (status, Encoding.UTF8.GetString(body.ToArray()));
    }

    private static async Task Write(NetworkStream stream, string value, CancellationToken token)
    {
        await stream.WriteAsync(Encoding.ASCII.GetBytes(value), token);
        await stream.FlushAsync(token);
    }

    // A single-request raw HTTP server avoids platform-specific HttpListener behavior and needs no native engine.
    private sealed class StubServer : IAsyncDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly CancellationTokenSource _stop = new();
        private readonly Task _serve;
        public string Url { get; }

        public StubServer(Func<NetworkStream, CancellationToken, Task> respond)
        {
            _listener.Start();
            Url = $"http://127.0.0.1:{((IPEndPoint)_listener.LocalEndpoint).Port}";
            _serve = Serve(respond);
        }

        private async Task Serve(Func<NetworkStream, CancellationToken, Task> respond)
        {
            try
            {
                using var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                using var stream = client.GetStream();
                using var reader = new StreamReader(stream, Encoding.ASCII, leaveOpen: true);
                var length = 0;
                while (await reader.ReadLineAsync(_stop.Token) is { Length: > 0 } line)
                {
                    if (line.StartsWith("Content-Length:", StringComparison.OrdinalIgnoreCase))
                    {
                        length = int.Parse(line.Split(':')[1].Trim());
                    }
                }
                var body = new char[length];
                await reader.ReadBlockAsync(body.AsMemory(), _stop.Token);
                await respond(stream, _stop.Token);
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
