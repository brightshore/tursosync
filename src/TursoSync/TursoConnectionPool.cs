using System.Collections.Concurrent;

namespace Turso.Sync;

/// <summary>
/// A small physical-connection pool for Turso, keyed by connection string. Opening a Turso connection is
/// expensive — the sync lane re-reads metadata/bootstraps (~2.5x a base open), and even a base open +
/// durable write costs ~80x a reused connection (measured). Consumers that open a connection per operation
/// pay that cost every time without pooling. ADO.NET's other providers (Npgsql, Microsoft.Data.Sqlite) pool
/// by default; this gives Turso the same.
/// </summary>
internal static class TursoConnectionPool
{
    /// <summary>Max idle physical connections kept per key when the connection string doesn't specify.</summary>
    internal const int DefaultMaxIdlePerKey = 4;

    /// <summary>
    /// How long a connection must have sat idle before renting re-runs the health probe. One returned moments
    /// ago was proven healthy on the way in, so probing it again on the way out is a wasted round trip on
    /// every checkout — and the stores open a connection per operation.
    /// </summary>
    private const long ProbeAfterIdleMs = 1000;

    private static readonly ConcurrentDictionary<string, ConcurrentQueue<TursoPhysicalConnection>> Pools = new(StringComparer.Ordinal);

    /// <summary>Rent a physical connection for <paramref name="config"/>, reusing an idle one if available.</summary>
    public static TursoPhysicalConnection Rent(string key, TursoSyncConfig config, bool forceSync) =>
        TryRentIdle(key) ?? TursoPhysicalConnection.Create(config, forceSync);

    /// <summary>An idle, healthy pooled connection for <paramref name="key"/>, or null if there is none. Never
    /// creates one, so it never waits on a sync database's gate.</summary>
    public static TursoPhysicalConnection? TryRentIdle(string key)
    {
        if (Pools.TryGetValue(key, out var queue))
        {
            while (queue.TryDequeue(out var pooled))
            {
                if (Environment.TickCount64 - pooled.PooledAtMs < ProbeAfterIdleMs || pooled.IsUsable())
                {
                    return pooled;
                }

                // An unusable connection has already left the queue, so it must be DISPOSED, not dropped:
                // letting it go unreferenced leaks the native connection and its reference on the shared sync
                // engine, so the engine's refcount never reaches zero, the replica's files stay open for the
                // life of the process, and ClearPool() can no longer release them for RemoteAttach to move.
                pooled.Dispose();
            }
        }

        return null;
    }

    /// <summary>Return a physical connection to the pool, or dispose it if the pool is full / it's unhealthy.</summary>
    public static void Return(string key, TursoPhysicalConnection physical, int maxIdle = DefaultMaxIdlePerKey)
    {
        // A connection that registered UDFs/collations or loaded extensions carries per-connection state
        // that must not leak to the next renter — drop it instead of pooling.
        if (physical.NonPoolable || physical.Raw.HasExtensions || !physical.IsUsable())
        {
            physical.Dispose();
            return;
        }

        var queue = Pools.GetOrAdd(key, _ => new ConcurrentQueue<TursoPhysicalConnection>());
        if (queue.Count >= (maxIdle > 0 ? maxIdle : DefaultMaxIdlePerKey))
        {
            physical.Dispose();
            return;
        }

        physical.PooledAtMs = Environment.TickCount64;
        queue.Enqueue(physical);
    }

    /// <summary>How many idle connections the pool holds for <paramref name="key"/>.</summary>
    public static int IdleCount(string key) => Pools.TryGetValue(key, out var queue) ? queue.Count : 0;

    /// <summary>Dispose and drop all pooled connections (test isolation / shutdown).</summary>
    public static void Clear()
    {
        foreach (var queue in Pools.Values)
        {
            while (queue.TryDequeue(out var physical))
            {
                physical.Dispose();
            }
        }

        Pools.Clear();
    }
}

/// <summary>
/// A physical Turso connection (the raw connection plus, for the sync lane, its owning sync database).
/// Pooled and reused across logical <see cref="TursoConnection"/> opens.
/// </summary>
internal sealed class TursoPhysicalConnection : IDisposable
{
    private TursoSyncDatabase? _syncDatabase;

    /// <summary>The <see cref="TursoSyncDatabaseCache"/> key to release on dispose, or null for the base
    /// (local) lane, which has no sync engine to share.</summary>
    private readonly string? _syncCacheKey;

    private TursoPhysicalConnection(TursoRawConnection raw, TursoSyncDatabase? syncDatabase, string? syncCacheKey)
    {
        Raw = raw;
        _syncDatabase = syncDatabase;
        _syncCacheKey = syncCacheKey;
    }

    public TursoRawConnection Raw { get; }

    /// <summary>When set, this physical connection is dropped rather than pooled on return.</summary>
    public bool NonPoolable { get; set; }

    /// <summary><see cref="Environment.TickCount64"/> when this was last returned to the pool, so renting can
    /// skip the health probe on a connection that was just proven healthy.</summary>
    public long PooledAtMs { get; set; }

    /// <summary>The owning sync database for the sync lane, or null for a base (local) connection.</summary>
    public TursoSyncDatabase? SyncDatabase => _syncDatabase;

    public static TursoPhysicalConnection Create(TursoSyncConfig config, bool forceSync = false)
    {
        // No remote and not forced → base local lane (no sync engine, no IO pump). Otherwise → sync engine.
        if (string.IsNullOrEmpty(config.RemoteUrl) && !forceSync)
        {
            return new TursoPhysicalConnection(TursoRawConnection.OpenLocal(config), null, null);
        }

        // Share one engine per replica rather than standing up a second owner of the same WAL. This sits here
        // rather than in the pool because the Pooling=false path reaches Create directly too, and the
        // one-engine-per-file-set invariant has to hold for it as well.
        var (key, db) = TursoSyncDatabaseCache.Acquire(config);
        try
        {
            return new TursoPhysicalConnection(TursoRawConnection.Open(db, config.BusyTimeoutMs), db, key);
        }
        catch
        {
            TursoSyncDatabaseCache.Release(key);
            throw;
        }
    }

    /// <summary>Cheap health probe: a trivial query must succeed for the connection to be reused.</summary>
    public bool IsUsable()
    {
        try
        {
            return Equals(Raw.QueryScalar("SELECT 1"), 1L);
        }
        catch (TursoException ex) when (ex.IsBusy || ex.IsBusySnapshot)
        {
            // Contention is not death. A checkpoint makes every concurrent statement fail, this probe
            // included — treating that as an unhealthy connection would throw away the whole pool exactly
            // when it is most contended, and rebuild it against the same busy engine.
            return true;
        }
        catch
        {
            return false;
        }
    }

    public void Dispose()
    {
        Raw.Dispose();

        // The engine is shared, so drop a reference rather than disposing it — the cache disposes it when the
        // last connection over that replica goes, which is what frees the file handles.
        if (_syncCacheKey is not null)
        {
            TursoSyncDatabaseCache.Release(_syncCacheKey);
        }

        _syncDatabase = null;
    }
}
