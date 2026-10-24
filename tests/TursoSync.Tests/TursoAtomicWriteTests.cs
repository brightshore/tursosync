using System.Collections.Concurrent;
using Turso.Sync;

namespace TursoSync.Tests;

/// <summary>
/// The host's whole-file write (the engine's <c>full_write</c> IO, used for the <c>-info</c> sidecar) must survive
/// concurrent writers of the same file, and readers of it.
///
/// <para>It wrote through a fixed <c>path + ".tmp"</c>. Two engines rewriting one replica's <c>-info</c> shared that
/// name, so one renamed the other's temp file into place and the other's rename found nothing to move:
/// <c>Could not find file '…-info'</c>, failing <c>sync_database_create</c>. That is what made
/// <c>SeparateDatabases_ConcurrentSiblings_StayConsistent</c> fail about one run in five. Measured with two writers
/// over four seconds: 1,359 such failures in ~25,000 writes, and readers never once saw the file missing, so the
/// rename itself was always atomic and the shared temp name was the whole defect.</para>
///
/// <para>The reader opens the file exactly as the engine's full read does (<see cref="TursoSyncDatabase.OpenForFullRead"/>),
/// because on Windows whether a rename may replace an open file depends on how that file was opened.</para>
/// </summary>
[TestClass]
public class TursoAtomicWriteTests
{
    private const int Writers = 4;
    private const int WritesEach = 150;

    [TestMethod]
    public void ConcurrentWriters_OfOneFile_AllSucceed_AndReadersOnlySeeWholeVersions()
    {
        var dir = Directory.CreateTempSubdirectory("tursosync-atomic-write-");
        try
        {
            var path = Path.Combine(dir.FullName, "store.db-info");
            // Each writer's content is distinct and self-describing, so a reader can tell a whole version from a
            // torn one: a run of one byte value, of a length that encodes that value.
            byte[] Version(int writer) => Enumerable.Repeat((byte)('a' + writer), 1000 + writer).ToArray();
            TursoSyncDatabase.WriteFileAtomically(path, Version(0));

            var failures = new ConcurrentQueue<string>();
            var writersLeft = Writers;
            var reads = 0;

            // Dedicated threads, not Task.Run: on a small CI runner busy writers can hold every pool thread, and the
            // reader then never runs at all.
            var reader = new Thread(() =>
            {
                while (Volatile.Read(ref writersLeft) > 0)
                {
                    try
                    {
                        using var stream = TursoSyncDatabase.OpenForFullRead(path);
                        if (stream is null)
                        {
                            // The engine would read this as empty: an atomic replace must never show it.
                            failures.Enqueue("read: the file was missing");
                            continue;
                        }

                        using var buffer = new MemoryStream();
                        stream.CopyTo(buffer);
                        var bytes = buffer.ToArray();
                        reads++;
                        var w = bytes.Length - 1000;
                        if (w is < 0 or >= Writers || bytes.Any(b => b != (byte)('a' + w)))
                        {
                            failures.Enqueue($"read: a torn version, {bytes.Length} bytes");
                        }
                    }
                    catch (Exception ex)
                    {
                        failures.Enqueue($"read: {ex.GetType().Name}: {ex.Message}");
                    }
                }
            }) { IsBackground = true };

            var writers = Enumerable.Range(0, Writers).Select(w => new Thread(() =>
            {
                try
                {
                    for (var i = 0; i < WritesEach; i++)
                    {
                        try
                        {
                            TursoSyncDatabase.WriteFileAtomically(path, Version(w));
                        }
                        catch (Exception ex)
                        {
                            failures.Enqueue($"write: {ex.GetType().Name}: {ex.Message}");
                        }
                    }
                }
                finally
                {
                    Interlocked.Decrement(ref writersLeft);
                }
            }) { IsBackground = true }).ToArray();

            reader.Start();
            foreach (var t in writers) t.Start();
            foreach (var t in writers) t.Join();
            reader.Join();

            reads.Should().BeGreaterThan(0, "the reader must have overlapped the writers for its result to mean anything");
            failures.Should().BeEmpty("concurrent replacements of one file must neither fail nor be seen half-written");
            Directory.GetFiles(dir.FullName, "*.tmp").Should().BeEmpty("every temp file is renamed into place or removed");
        }
        finally
        {
            try { dir.Delete(recursive: true); } catch { /* temp */ }
        }
    }
}
