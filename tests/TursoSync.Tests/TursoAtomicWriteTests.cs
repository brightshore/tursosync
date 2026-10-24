using System.Collections.Concurrent;
using Turso.Sync;

namespace TursoSync.Tests;

/// <summary>
/// The host's whole-file write (the engine's <c>full_write</c> IO, used for the <c>-info</c> sidecar) must survive
/// concurrent writers of the same file.
///
/// <para>It wrote through a fixed <c>path + ".tmp"</c>. Two engines rewriting one replica's <c>-info</c> shared that
/// name, so one renamed the other's temp file into place and the other's rename found nothing to move:
/// <c>Could not find file '…-info'</c>, failing <c>sync_database_create</c>. That is what made
/// <c>SeparateDatabases_ConcurrentSiblings_StayConsistent</c> fail about one run in five. Measured with two writers
/// over four seconds: 1,359 such failures in ~25,000 writes, and readers never once saw the file missing, so the
/// rename itself was always atomic and the shared temp name was the whole defect.</para>
/// </summary>
[TestClass]
public class TursoAtomicWriteTests
{
    private const int Writers = 4;
    private static readonly TimeSpan RunFor = TimeSpan.FromMilliseconds(800);

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

            var stop = DateTime.UtcNow + RunFor;
            var failures = new ConcurrentQueue<string>();
            var writes = 0;
            var writers = Enumerable.Range(0, Writers).Select(w => Task.Run(() =>
            {
                while (DateTime.UtcNow < stop)
                {
                    try
                    {
                        TursoSyncDatabase.WriteFileAtomically(path, Version(w));
                        Interlocked.Increment(ref writes);
                    }
                    catch (Exception ex)
                    {
                        failures.Enqueue($"write: {ex.GetType().Name}: {ex.Message}");
                    }
                }
            })).ToArray();

            var reads = 0;
            var reader = Task.Run(() =>
            {
                while (DateTime.UtcNow < stop)
                {
                    try
                    {
                        var bytes = File.ReadAllBytes(path);
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
            });

            Task.WaitAll([.. writers, reader]);

            writes.Should().BeGreaterThan(Writers * 10, "the writers must actually have contended for the result to mean anything");
            reads.Should().BeGreaterThan(10);
            failures.Should().BeEmpty("concurrent replacements of one file must neither fail nor be seen half-written");
            Directory.GetFiles(dir.FullName, "*.tmp").Should().BeEmpty("every temp file is renamed into place or removed");
        }
        finally
        {
            try { dir.Delete(recursive: true); } catch { /* temp */ }
        }
    }
}
