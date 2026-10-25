using System.Net;
using System.Net.Sockets;
using System.Text;
using Turso.Sync;

namespace TursoSync.Tests;

[TestClass]
public class ZzRawRequestProbe
{
    [TestMethod]
    public void Dump_the_bootstrap_request()
    {
        var tcp = new TcpListener(IPAddress.Loopback, 0);
        tcp.Start();
        var port = ((IPEndPoint)tcp.LocalEndpoint).Port;
        var captured = new StringBuilder();
        var serve = Task.Run(async () =>
        {
            using var client = await tcp.AcceptTcpClientAsync();
            var stream = client.GetStream();
            var buf = new byte[65536];
            var n = await stream.ReadAsync(buf);
            for (var i = 0; i < n; i++)
            {
                var b = buf[i];
                captured.Append(b is >= 0x20 and < 0x7f ? ((char)b).ToString() : b == '\n' ? "\\n\n" : $"\\x{b:x2}");
            }
            var resp = Encoding.ASCII.GetBytes("HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
            await stream.WriteAsync(resp);
        });
        var dir = Path.Combine(Path.GetTempPath(), "probe-" + Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(dir);
        try
        {
            try
            {
                using var db = TursoSyncDatabase.Create(new TursoSyncConfig
                {
                    Path = Path.Combine(dir, "x.db"),
                    RemoteUrl = $"http://localhost:{port}",
                    BootstrapIfEmpty = true,
                });
            }
            catch (TursoException) { }
            serve.Wait(5000);
            Assert.Fail("RAW REQUEST:\n" + captured);
        }
        finally
        {
            tcp.Stop();
            try { Directory.Delete(dir, true); } catch { }
        }
    }
}
