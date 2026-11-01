using System.Runtime.InteropServices;
using Turso.Sync;

namespace TursoSync.Tests;

/// <summary>
/// Unit coverage for the sync engine's HTTP request marshaling — <see cref="TursoSyncDatabase.BodyFromSlice"/>
/// and <see cref="TursoSyncDatabase.BuildHttpMessage"/>. These lock the regression behind the cloud-only 400:
/// an empty-but-present request body (the initial <c>/pull-updates</c> protobuf encodes to zero bytes) must
/// still be sent as a body so its <c>content-type</c> header and <c>Content-Length: 0</c> reach the server.
/// Pure and native-free, so they always run (no gating).
/// </summary>
[TestClass]
public class TursoSyncHttpMessageTests
{
    private static readonly IReadOnlyList<KeyValuePair<string, string>> NoHeaders = [];

    [TestMethod]
    public void BodyFromSlice_NullPointer_IsNoBody()
    {
        // A true None from the engine: no request body at all.
        var slice = new TursoSlice { Ptr = IntPtr.Zero, Len = 0 };
        TursoSyncDatabase.BodyFromSlice(slice).Should().BeNull();
    }

    [TestMethod]
    public void BodyFromSlice_NonNullPointerZeroLength_IsEmptyBody()
    {
        // Some(empty): a dangling-but-non-null slice (0x1, len 0), as the sdk-kit yields for a zero-byte
        // protobuf. Must marshal to an empty — not null — body so BuildHttpMessage still attaches Content.
        var slice = new TursoSlice { Ptr = new IntPtr(1), Len = 0 };
        TursoSyncDatabase.BodyFromSlice(slice).Should().NotBeNull().And.BeEmpty();
    }

    [TestMethod]
    public void BodyFromSlice_NonNullPointerWithBytes_MarshalsBytes()
    {
        var bytes = new byte[] { 1, 2, 3, 4 };
        var handle = GCHandle.Alloc(bytes, GCHandleType.Pinned);
        try
        {
            var slice = new TursoSlice { Ptr = handle.AddrOfPinnedObject(), Len = (nuint)bytes.Length };
            TursoSyncDatabase.BodyFromSlice(slice).Should().Equal(bytes);
        }
        finally
        {
            handle.Free();
        }
    }

    [TestMethod]
    public void BuildHttpMessage_EmptyBody_SendsContentAndPreservesContentType()
    {
        // The regression: a zero-length body must still yield Content, so the protobuf content-type lands and
        // Content-Length: 0 is sent. Previously the empty body was dropped, the content-type header (a content
        // header) had nowhere to attach, and Turso Cloud rejected the request with HTTP 400.
        var headers = new[] { new KeyValuePair<string, string>("content-type", "application/protobuf") };

        using var message = TursoSyncDatabase.BuildHttpMessage(
            "POST", "https://example.turso.io/pull-updates", [], headers, authToken: null, host: null);

        message.Content.Should().NotBeNull();
        message.Content!.Headers.ContentLength.Should().Be(0);
        message.Content.Headers.ContentType!.MediaType.Should().Be("application/protobuf");
    }

    [TestMethod]
    public void BuildHttpMessage_NoBody_HasNoContent()
    {
        using var message = TursoSyncDatabase.BuildHttpMessage(
            "GET", "https://example.turso.io/info", body: null, NoHeaders, authToken: null, host: null);

        message.Content.Should().BeNull();
    }

    [TestMethod]
    public void BuildHttpMessage_AttachesBearerTokenAndDefaultUserAgent()
    {
        using var message = TursoSyncDatabase.BuildHttpMessage(
            "GET", "https://example.turso.io/info", body: null, NoHeaders, authToken: "secret-token", host: null);

        message.Headers.GetValues("Authorization").Should().ContainSingle().Which.Should().Be("Bearer secret-token");
        message.Headers.GetValues("User-Agent").Should().ContainSingle().Which.Should().Be("tursosync");
    }

    [TestMethod]
    public void BuildHttpMessage_DoesNotOverrideCallerUserAgent()
    {
        var headers = new[] { new KeyValuePair<string, string>("User-Agent", "custom-agent") };

        using var message = TursoSyncDatabase.BuildHttpMessage(
            "GET", "https://example.turso.io/info", body: null, headers, authToken: null, host: null);

        message.Headers.GetValues("User-Agent").Should().ContainSingle().Which.Should().Be("custom-agent");
    }

    [TestMethod]
    [DataRow("application/protobuf")]
    [DataRow("application/json")]
    public void BuildHttpMessage_NegotiatesGzip_InsteadOfNativeMimeEncoding(string mime)
    {
        var headers = new[] { new KeyValuePair<string, string>("AcCePt-EnCoDiNg", mime) };
        using var message = TursoSyncDatabase.BuildHttpMessage(
            "POST", "https://example.turso.io/pull-updates", [], headers, authToken: null, host: null);

        message.Headers.AcceptEncoding.Should().ContainSingle().Which.Value.Should().Be("gzip");
        message.Headers.Accept.Should().ContainSingle().Which.MediaType.Should().Be(mime);
    }

    [TestMethod]
    public void BuildHttpMessage_NegotiatesOnlySupportedEncoding_AndPreservesAccept()
    {
        var headers = new[]
        {
            new KeyValuePair<string, string>("Accept", "application/json"),
            new KeyValuePair<string, string>("Accept-Encoding", "br, gzip"),
        };
        using var message = TursoSyncDatabase.BuildHttpMessage(
            "GET", "https://example.turso.io/info", null, headers, authToken: null, host: null);

        message.Headers.AcceptEncoding.Should().ContainSingle().Which.Value.Should().Be("gzip");
        message.Headers.Accept.Should().ContainSingle().Which.MediaType.Should().Be("application/json");
    }

    [TestMethod]
    public void BuildHttpMessage_SetsHost()
    {
        using var message = TursoSyncDatabase.BuildHttpMessage(
            "GET", "https://example.turso.io/info", body: null, NoHeaders, authToken: null, host: "ns.turso.io");

        message.Headers.Host.Should().Be("ns.turso.io");
    }

    [TestMethod]
    public void BuildHttpMessage_BodyWithBytes_RoundTrips()
    {
        var body = new byte[] { 9, 8, 7 };

        using var message = TursoSyncDatabase.BuildHttpMessage(
            "POST", "https://example.turso.io/pull-updates", body, NoHeaders, authToken: null, host: null);

        message.Content.Should().NotBeNull();
        message.Content!.ReadAsByteArrayAsync().GetAwaiter().GetResult().Should().Equal(body);
    }
}
