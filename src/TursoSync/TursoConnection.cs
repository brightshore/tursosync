using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace Turso.Sync;

/// <summary>
/// An ADO.NET <see cref="DbConnection"/> over a synced Turso database, at parity with the official
/// <c>Turso.Data</c> connection but opened through the sync engine. The connection string carries the local
/// path plus optional sync settings: <c>Data Source=…;Remote Url=…;Auth Token=…;Namespace=…;Bootstrap=false</c>.
/// </summary>
public sealed class TursoConnection : DbConnection
{
    private TursoConnectionStringBuilder _options;
    private TursoPhysicalConnection? _physical;
    private bool _disposed;
    private bool _readUncommitted;

    /// <summary>Create an unopened connection.</summary>
    public TursoConnection() : this(string.Empty)
    {
    }

    /// <summary>Create a connection for <paramref name="connectionString"/>.</summary>
    public TursoConnection(string connectionString) =>
        _options = new TursoConnectionStringBuilder(connectionString);

    /// <inheritdoc/>
    [AllowNull]
    public override string ConnectionString
    {
        get => _options.ConnectionString;
        set
        {
            if (State == ConnectionState.Open)
            {
                throw new InvalidOperationException("ConnectionString cannot be set while the connection is open.");
            }

            _options = new TursoConnectionStringBuilder(value ?? string.Empty);
        }
    }

    /// <inheritdoc/>
    public override string Database => "main";

    /// <inheritdoc/>
    public override string DataSource => _options.DataSource;

    /// <inheritdoc/>
    public override string ServerVersion => typeof(TursoConnection).Assembly.GetName().Version?.ToString() ?? "0.0.0";

    /// <inheritdoc/>
    public override ConnectionState State => _physical is null ? ConnectionState.Closed : ConnectionState.Open;

    /// <inheritdoc/>
    protected override DbProviderFactory DbProviderFactory => TursoFactory.Instance;

    /// <summary>The underlying synced database, or null for a base (local) connection / when closed.</summary>
    public TursoSyncDatabase? SyncDatabase => _physical?.SyncDatabase;

    internal TursoRawConnection Raw => _physical?.Raw ?? throw new InvalidOperationException("Turso database is closed.");

    /// <summary>Dispose and drop all pooled physical connections (test isolation / shutdown).</summary>
    public static void ClearPool() => TursoConnectionPool.Clear();

    /// <summary>
    /// Replicas whose sync engine is currently held open, with the number of connections holding each.
    ///
    /// <para>One engine is shared per replica file set and lives until its last connection is disposed — which
    /// is what releases the replica's file handles. A <see cref="TursoConnection"/> that is never closed
    /// therefore pins its replica for the life of the process, and no amount of <see cref="ClearPool"/> will
    /// free it. Assert this is empty after closing everything to catch that.</para>
    /// </summary>
    public static IReadOnlyList<TursoOpenReplica> OpenReplicas => TursoSyncDatabaseCache.Snapshot();

    internal int DefaultTimeout => _options.DefaultTimeout;

    internal bool ReadUncommitted
    {
        get => _readUncommitted;
        set => _readUncommitted = value;
    }

    /// <inheritdoc/>
    public override void Open()
    {
        ThrowIfCannotOpen();

        // Connection opens are expensive on Turso (especially the sync lane's metadata bootstrap), and the
        // store opens per operation — so pool physical connections by default, like Npgsql/SQLite do. Lane
        // selection (base vs sync) happens inside the physical connection: no remote → base local fast path.
        _physical = RentIdle() ?? CreatePhysical();
    }

    /// <summary>
    /// Open the connection without blocking the caller on a cold open.
    ///
    /// <para>An idle pooled connection is rented synchronously: that is cheap, and it is the common case. A new
    /// physical connection is created on the thread pool instead, because creating one can block for as long as
    /// the network takes: on the sync lane it connects through <see cref="TursoSyncDatabase.Connect"/>, which
    /// waits for the database's gate, and a push or pull holds that gate across its whole HTTP round trip. A
    /// caller that awaits this from a UI thread therefore waits without freezing, where before the whole open
    /// ran on its thread.</para>
    /// </summary>
    public override async Task OpenAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ThrowIfCannotOpen();

        if (RentIdle() is { } idle)
        {
            _physical = idle;
            return;
        }

        var created = await Task.Run(CreatePhysical, cancellationToken).ConfigureAwait(false);
        if (_disposed)
        {
            // Disposed while the open was in flight: nothing will ever close this one, so release it now.
            created.Dispose();
            throw new ObjectDisposedException(GetType().FullName);
        }

        _physical = created;
    }

    private void ThrowIfCannotOpen()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_physical is not null)
        {
            throw new InvalidOperationException("The connection is already open.");
        }
    }

    /// <summary>An idle pooled physical connection, or null when pooling is off or none is idle.</summary>
    private TursoPhysicalConnection? RentIdle() =>
        _options.Pooling ? TursoConnectionPool.TryRentIdle(_options.PoolKey) : null;

    private TursoPhysicalConnection CreatePhysical() =>
        TursoPhysicalConnection.Create(_options.ToConfig(), _options.Sync);

    /// <inheritdoc/>
    public override void Close()
    {
        if (_physical is not null)
        {
            if (_options.Pooling)
            {
                TursoConnectionPool.Return(_options.PoolKey, _physical, _options.MaxIdleConnections);
            }
            else
            {
                _physical.Dispose();
            }

            _physical = null;
        }

        _readUncommitted = false;
    }

    /// <inheritdoc/>
    public override void ChangeDatabase(string databaseName) =>
        throw new NotSupportedException("Turso does not support changing the active database.");

    /// <summary>Execute non-query SQL directly on this connection.</summary>
    public int ExecuteNonQuery(string sql)
    {
        using var command = CreateCommand();
        command.CommandText = sql;
        return command.ExecuteNonQuery();
    }

    /// <summary>
    /// Register a scalar user-defined function (or remove it when <paramref name="function"/> is null).
    /// A connection with registered functions is not returned to the pool.
    /// </summary>
    public void CreateFunction(string name, int argc, Func<object?[], object?>? function, bool deterministic = true)
    {
        Raw.RegisterScalarFunction(name, argc, deterministic, function);
        MarkNonPoolable();
    }

    /// <summary>Register an aggregate function (or remove it when <paramref name="step"/> is null).</summary>
    public void CreateAggregate(string name, int argc, object? seed, Func<object?, object?[], object?>? step, Func<object?, object?> finalize)
    {
        Raw.RegisterAggregateFunction(name, argc, seed, step, finalize);
        MarkNonPoolable();
    }

    /// <summary>Register a collation (or remove it when <paramref name="comparison"/> is null).</summary>
    public void CreateCollation(string name, Func<string, string, int>? comparison)
    {
        Raw.RegisterCollation(name, comparison);
        MarkNonPoolable();
    }

    /// <summary>Enable or disable loadable extensions on this connection.</summary>
    public void EnableExtensions(bool enabled)
    {
        Raw.EnableLoadExtension(enabled);
        MarkNonPoolable();
    }

    /// <summary>Load a SQLite extension from <paramref name="path"/>.</summary>
    public void LoadExtension(string path)
    {
        Raw.LoadExtension(path);
        MarkNonPoolable();
    }

    private void MarkNonPoolable()
    {
        if (_physical is not null)
        {
            _physical.NonPoolable = true;
        }
    }

    /// <inheritdoc/>
    protected override DbCommand CreateDbCommand() => new TursoCommand(this);

    /// <inheritdoc/>
    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        if (_physical is null)
        {
            throw new InvalidOperationException("Turso database is closed.");
        }

        return new TursoTransaction(this, isolationLevel);
    }

    /// <inheritdoc/>
    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Close();
        }

        _disposed = true;
        base.Dispose(disposing);
    }
}

/// <summary>A transaction over a <see cref="TursoConnection"/> (SQLite-style BEGIN/COMMIT/ROLLBACK).</summary>
public sealed class TursoTransaction : DbTransaction
{
    private readonly TursoConnection _connection;
    private bool _completed;

    internal TursoTransaction(TursoConnection connection, IsolationLevel isolationLevel)
    {
        _connection = connection;
        IsolationLevel = NormalizeIsolationLevel(isolationLevel);
        if (IsolationLevel == IsolationLevel.ReadUncommitted)
        {
            connection.ReadUncommitted = true;
        }

        connection.ExecuteNonQuery("BEGIN");
    }

    /// <inheritdoc/>
    public override IsolationLevel IsolationLevel { get; }

    internal bool IsCompleted => _completed;

    /// <inheritdoc/>
    protected override DbConnection DbConnection => _connection;

    /// <inheritdoc/>
    public override void Commit()
    {
        ThrowIfCompleted();
        _connection.ExecuteNonQuery("COMMIT");
        Complete();
    }

    /// <inheritdoc/>
    public override void Rollback()
    {
        ThrowIfCompleted();
        try
        {
            _connection.ExecuteNonQuery("ROLLBACK");
        }
        finally
        {
            Complete();
        }
    }

    /// <inheritdoc/>
    protected override void Dispose(bool disposing)
    {
        if (disposing && !_completed)
        {
            Rollback();
        }

        base.Dispose(disposing);
    }

    private void Complete()
    {
        if (IsolationLevel == IsolationLevel.ReadUncommitted)
        {
            _connection.ReadUncommitted = false;
        }

        _completed = true;
    }

    private void ThrowIfCompleted()
    {
        if (_completed)
        {
            throw new InvalidOperationException("This transaction has already completed.");
        }
    }

    private static IsolationLevel NormalizeIsolationLevel(IsolationLevel isolationLevel) => isolationLevel switch
    {
        IsolationLevel.Unspecified => IsolationLevel.Serializable,
        IsolationLevel.Serializable => IsolationLevel.Serializable,
        IsolationLevel.ReadCommitted => IsolationLevel.Serializable,
        IsolationLevel.ReadUncommitted => IsolationLevel.ReadUncommitted,
        _ => throw new NotSupportedException($"Isolation level {isolationLevel} is not supported."),
    };
}
