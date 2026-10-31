namespace Turso.Sync;

/// <summary>A replica whose sync engine is currently held open, and how many connections are holding it.</summary>
/// <param name="Path">Absolute path of the replica's database file.</param>
/// <param name="Connections">Live connections sharing its engine, including idle pooled ones.</param>
public sealed record TursoOpenReplica(string Path, int Connections);

/// <summary>
/// One <see cref="TursoSyncDatabase"/> per replica file set, shared by every connection over it.
///
/// <para><b>Why.</b> A sync database owns its replica's WAL — it fragments it, rewinds it to a watermark on a
/// pull-side revert, and truncates it on checkpoint. Standing up a second engine over the same files gives
/// that file set two owners with independent views. Measured against a live remote under 4-way concurrent
/// load (see <c>TursoWalSharingTests</c>): an engine per connection produced 745k errors and 86 failed
/// checkpoints, including <c>sync_database_create ... database tape error: database is busy</c> — engines
/// that could not be opened at all while another was active. One shared engine over the same load: the same
/// statement throughput, a third of the errors, zero failed checkpoints, and the busy class gone entirely.</para>
///
/// <para>The engine is designed for this — <see cref="TursoSyncDatabase.Connect"/> exists to hand out
/// connections, and the class guards its own operations behind an internal lock. Entries are reference
/// counted by live <see cref="TursoPhysicalConnection"/>s (including idle pooled ones, so a pooled connection
/// keeps the engine warm and avoids re-paying the bootstrap); the engine is disposed when the last one goes,
/// which is what releases the replica's file handles for <c>RemoteAttach</c>.</para>
/// </summary>
internal static class TursoSyncDatabaseCache
{
    private sealed class Entry
    {
        public required TursoSyncDatabase Database { get; init; }

        /// <summary>The remote this engine is bound to, so a second config for the same files that disagrees
        /// is rejected rather than silently handed an engine pointing somewhere else.</summary>
        public required string RemoteIdentity { get; init; }

        /// <summary>Batch size fixed when the shared engine was created. Later connections must agree.</summary>
        public required int PushOperationsThreshold { get; init; }

        public int RefCount { get; set; }
    }

    private static readonly Dictionary<string, Entry> Entries = new(StringComparer.Ordinal);
    /// <summary>Held while an engine is acquired or released, including while <see cref="TursoSyncDatabase.Create"/>
    /// runs (a bootstrap download among it). Tests hold it to stand in for that.</summary>
    internal static readonly object Gate = new();

    /// <summary>
    /// Get the engine for <paramref name="config"/>'s replica, creating it if this is the first caller, and
    /// take a reference. Every successful call must be paired with <see cref="Release"/>.
    /// </summary>
    /// <returns>The cache key to release with, and the shared engine.</returns>
    public static (string Key, TursoSyncDatabase Database) Acquire(TursoSyncConfig config)
    {
        var key = KeyFor(config);
        var identity = RemoteIdentityOf(config);

        lock (Gate)
        {
            if (Entries.TryGetValue(key, out var existing))
            {
                if (!string.Equals(existing.RemoteIdentity, identity, StringComparison.Ordinal))
                {
                    throw new InvalidOperationException(
                        $"The replica at '{key}' is already open against remote '{existing.RemoteIdentity}'; "
                        + $"refusing to also open it against '{identity}'. Close the existing connections first.");
                }

                if (existing.PushOperationsThreshold != config.PushOperationsThreshold)
                {
                    throw new InvalidOperationException(
                        $"The replica at '{key}' is already open with Push Operations Threshold "
                        + $"{existing.PushOperationsThreshold}; refusing to also open it with {config.PushOperationsThreshold}. "
                        + "Close the existing connections and call TursoConnection.ClearPool() before changing the threshold.");
                }

                existing.RefCount++;
                return (key, existing.Database);
            }

            // Created under the gate: concurrent creation over one replica is exactly what fails with
            // "database tape error: database is busy", and there is nothing to gain by racing here.
            var database = TursoSyncDatabase.Create(config);
            Entries[key] = new Entry
            {
                Database = database, RemoteIdentity = identity,
                PushOperationsThreshold = config.PushOperationsThreshold, RefCount = 1,
            };
            return (key, database);
        }
    }

    /// <summary>Drop a reference taken by <see cref="Acquire"/>, disposing the engine when the last one goes.</summary>
    public static void Release(string key)
    {
        TursoSyncDatabase? toDispose = null;
        lock (Gate)
        {
            if (!Entries.TryGetValue(key, out var entry))
            {
                return;
            }

            if (--entry.RefCount <= 0)
            {
                Entries.Remove(key);
                toDispose = entry.Database;
            }
        }

        // Disposed outside the gate: teardown drives engine IO and must not block other replicas' acquires.
        toDispose?.Dispose();
    }

    /// <summary>Dispose every cached engine regardless of reference count (test isolation / shutdown).</summary>
    public static void Clear()
    {
        Entry[] entries;
        lock (Gate)
        {
            entries = [.. Entries.Values];
            Entries.Clear();
        }

        foreach (var entry in entries)
        {
            entry.Database.Dispose();
        }
    }

    /// <summary>Live engine count. Test seam for asserting that sharing actually happened.</summary>
    internal static int Count
    {
        get { lock (Gate) { return Entries.Count; } }
    }

    /// <summary>Snapshot of every replica currently held open, newest state at call time.</summary>
    internal static IReadOnlyList<TursoOpenReplica> Snapshot()
    {
        lock (Gate)
        {
            return [.. Entries.Select(e => new TursoOpenReplica(e.Key, e.Value.RefCount))];
        }
    }

    /// <summary>How many connections currently share the engine for <paramref name="path"/> (0 when none).
    /// Test seam — scoped to one replica, so it is unaffected by other replicas in the cache.</summary>
    internal static int RefCountFor(string path)
    {
        var key = Path.GetFullPath(path);
        lock (Gate)
        {
            return Entries.TryGetValue(key, out var entry) ? entry.RefCount : 0;
        }
    }

    /// <summary>
    /// Identity of the replica: its absolute path. The file set is the thing that can only have one owner, so
    /// two differently-spelled connection strings for the same file must land on the same engine.
    /// </summary>
    private static string KeyFor(TursoSyncConfig config) =>
        string.IsNullOrEmpty(config.Path) ? string.Empty : Path.GetFullPath(config.Path);

    /// <summary>Remote URL + namespace. Deliberately NOT the auth token — tokens rotate, and a refreshed
    /// token for the same database must reuse the engine rather than be rejected as a conflict.</summary>
    private static string RemoteIdentityOf(TursoSyncConfig config) =>
        $"{config.RemoteUrl}|{config.Namespace}";
}
