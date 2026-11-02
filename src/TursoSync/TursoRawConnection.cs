using System.Runtime.InteropServices;
using System.Text;

namespace Turso.Sync;

/// <summary>
/// A SQL connection over a synced Turso database. Wraps a raw <c>turso_connection_t</c> and drives the
/// statement step loop, pumping one sync-IO item before each local statement-IO step (the ordering the Go
/// base driver uses: <c>extraIo()</c> then <c>turso_statement_run_io</c>). This is the primitive the
/// ADO.NET layer is built on.
/// </summary>
public sealed class TursoRawConnection : IDisposable
{
    private readonly TursoSyncDatabase? _syncDatabase;
    private readonly Action? _pump;
    private readonly List<GCHandle> _extensionHandles = [];
    private IntPtr _connection;
    private IntPtr _baseDatabase;
    private bool _disposed;

    /// <summary>How long a contended prepare may wait for the lock (<see cref="Prepare"/>); 0 or less never waits.</summary>
    private readonly int _busyTimeoutMs;

    static TursoRawConnection() => TursoNativeLibrary.EnsureResolver();

    private TursoRawConnection(IntPtr connection, TursoSyncDatabase? syncDatabase, IntPtr baseDatabase, Action? pump, int busyTimeoutMs)
    {
        _connection = connection;
        _syncDatabase = syncDatabase;
        _baseDatabase = baseDatabase;
        _pump = pump;
        _busyTimeoutMs = busyTimeoutMs;
    }

    // NOTE — a managed busy-retry was tried here on 2026-08-01 and REVERTED; do not reintroduce it without
    // reading this. Confirmed against the engine source in reference/turso-main:
    //
    //  * The busy handler is real and it works, but it lives in Statement::step (core/statement.rs). On a busy
    //    step it invokes the handler and, if the handler says wait, converts the result to StepResult::IO and
    //    yields — so the wait is driven by the caller's IO pump, which Step() below already does. Genuine
    //    Busy during step is therefore handled correctly today.
    //  * It is NOT consulted while preparing. A contended prepare_single returns Busy straight away: before
    //    Prepare() retried it, TursoBusyTimeoutTests measured a ~3 microsecond median time-to-failure whether
    //    the connection's busy timeout was 200 ms or 5000 ms. The proper home for that wait is the engine's
    //    prepare path; until it has one, Prepare() below supplies it outside a transaction (see the end of
    //    this note).
    //  * BusySnapshot is a DIFFERENT error and must never be retried in place. The engine is explicit:
    //    "Retrying with busy_timeout will NEVER HELP" (core/storage/wal.rs) — a checkpoint advanced the WAL,
    //    the read snapshot is permanently stale, and only rolling back and restarting the transaction gets a
    //    fresh one.
    //
    // The reverted retry ignored that last point: it reset and re-stepped on the same stale snapshot, so every
    // contended statement spun until its deadline instead of ever succeeding. With a checkpoint every 100 ms a
    // 3-second probe arm ran indefinitely at 165% CPU, versus 3.0 s with retry off (650 ok / 55,740 busy).
    // Retry-off + no checkpoint and retry-on + no checkpoint both complete cleanly — it is specifically
    // retrying a stale snapshot underneath a checkpoint that wedges.
    //
    // What IS retried now is narrower, and sound for the same reason Connect's retry is: Prepare() waits out a
    // busy prepare_single, but only while the connection is in AUTOCOMMIT. A prepare that failed outside a
    // transaction leaves nothing behind (no statement, no snapshot), so the next attempt starts fresh and sees
    // the WAL as the finished checkpoint left it. Inside a transaction the connection holds a snapshot a
    // checkpoint may have made stale, so there a busy prepare still fails at once, as it always has, and the
    // caller rolls back and restarts. Step is still never retried here.

    /// <summary>The owning synced database, or null for a base (local, non-sync) connection.</summary>
    public TursoSyncDatabase? SyncDatabase => _syncDatabase;

    /// <summary>True if this connection drives the sync IO pump (i.e. has a remote).</summary>
    public bool IsSynced => _pump is not null;

    /// <summary>True if any UDF/aggregate/collation has been registered on this connection.</summary>
    public bool HasExtensions => _extensionHandles.Count > 0;

    /// <summary>True if a transaction is open on this connection.</summary>
    internal bool InTransaction => !TursoNative.ConnectionGetAutocommit(_connection);

    /// <summary>Open a connection to a synced <paramref name="database"/> (pumps sync IO per statement step).</summary>
    public static TursoRawConnection Open(TursoSyncDatabase database, int busyTimeoutMs = 5000)
    {
        ArgumentNullException.ThrowIfNull(database);
        return new TursoRawConnection(
            database.Connect(busyTimeoutMs), database, IntPtr.Zero, database.ProcessOneIo,
            busyTimeoutMs == 0 ? 5000 : busyTimeoutMs);
    }

    /// <summary>
    /// Open a base, local-only connection through the plain Turso engine (<c>AsyncIO=0</c>, no sync engine,
    /// no IO pump). This is the offline fast path — same SQLite-format storage, none of the sync overhead.
    /// </summary>
    public static TursoRawConnection OpenLocal(TursoSyncConfig config)
    {
        ArgumentNullException.ThrowIfNull(config);

        var (dbConfig, toFree) = TursoConfigMarshal.BuildDatabaseConfig(config, asyncIo: 0);
        IntPtr database;
        try
        {
            TursoSyncDatabase.Check(TursoNative.DatabaseNew(ref dbConfig, out database, out var errNew), errNew, "database_new");
        }
        finally
        {
            TursoConfigMarshal.Free(toFree);
        }

        try
        {
            TursoSyncDatabase.Check(TursoNative.DatabaseOpen(database, out var errOpen), errOpen, "database_open");
            TursoSyncDatabase.Check(TursoNative.DatabaseConnect(database, out var connection, out var errConn), errConn, "database_connect");

            var timeout = config.BusyTimeoutMs == 0 ? 5000 : config.BusyTimeoutMs;
            if (timeout > 0)
            {
                TursoNative.ConnectionSetBusyTimeoutMs(connection, timeout);
            }

            return new TursoRawConnection(connection, null, database, pump: null, timeout);
        }
        catch
        {
            TursoNative.DatabaseDeinit(database);
            throw;
        }
    }

    /// <summary>
    /// Prepare a single statement.
    ///
    /// <para>Outside a transaction, a prepare that finds the database locked (a checkpoint holds it, typically)
    /// is retried until the connection's busy timeout runs out, the wait the engine's own busy handler gives a
    /// step but not a prepare. Inside a transaction it is not: lock contention surfaces at once as a
    /// <see cref="TursoException"/> with <see cref="TursoException.IsBusy"/> or
    /// <see cref="TursoException.IsBusySnapshot"/> set, and the transaction must be restarted. The note above
    /// says why the two differ.</para>
    /// </summary>
    public TursoRawStatement Prepare(string sql)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(sql);

        var deadline = Environment.TickCount64 + Math.Max(0, _busyTimeoutMs);
        var backoffMs = 1;
        while (true)
        {
            try
            {
                return PrepareOnce(sql);
            }
            catch (TursoException ex) when ((ex.IsBusy || ex.IsBusySnapshot) && !InTransaction)
            {
                if (!WaitForRetry(deadline, ref backoffMs))
                {
                    throw;
                }
            }
        }
    }

    /// <summary>
    /// Roll back the open transaction, waiting out a busy failure for up to the busy timeout. Unlike the
    /// statements inside a transaction this is sound to retry, since a rollback reads nothing from the snapshot
    /// that may have gone stale. It is worth retrying because a checkpoint fails a ROLLBACK as readily as any
    /// other statement, and a connection left in its transaction keeps whatever locks it took.
    /// </summary>
    internal void Rollback()
    {
        var deadline = Environment.TickCount64 + Math.Max(0, _busyTimeoutMs);
        var backoffMs = 1;
        while (true)
        {
            try
            {
                RollbackStarting?.Invoke();
                Execute("ROLLBACK");
                return;
            }
            catch (TursoException ex) when ((ex.IsBusy || ex.IsBusySnapshot) && InTransaction)
            {
                if (!WaitForRetry(deadline, ref backoffMs))
                {
                    throw;
                }
            }
        }
    }

    /// <summary>Runs before each attempt in <see cref="Rollback"/>. A test seam: throwing from it is how a test makes
    /// a rollback fail, which otherwise takes a checkpoint outlasting the busy timeout.</summary>
    internal Action? RollbackStarting { get; set; }

    /// <summary>Back off before another attempt, or false when the deadline leaves no time for one. Same rules as
    /// <see cref="TursoSyncDatabase.Connect"/>: the busy timeout bounds the whole operation, so never sleep past the
    /// deadline and never attempt once it has passed.</summary>
    private static bool WaitForRetry(long deadline, ref int backoffMs)
    {
        var remaining = deadline - Environment.TickCount64;
        if (remaining <= 0)
        {
            return false;
        }

        Thread.Sleep((int)Math.Min(backoffMs, remaining));
        if (Environment.TickCount64 >= deadline)
        {
            return false;
        }

        backoffMs = Math.Min(backoffMs * 2, 50);
        return true;
    }

    private TursoRawStatement PrepareOnce(string sql)
    {
        var gate = _syncDatabase?.ApplyGate;
        ApplyGateHolds.Enter(gate);
        TursoStatus status;
        IntPtr stmt, errorPtr;
        try
        {
            status = TursoNative.ConnectionPrepareSingle(_connection, sql, out stmt, out errorPtr);
        }
        finally
        {
            ApplyGateHolds.Exit(gate);
        }

        TursoSyncDatabase.Check(status, errorPtr, "prepare_single");
        return new TursoRawStatement(stmt, _pump, gate);
    }

    /// <summary>Execute non-query SQL and return rows affected.</summary>
    public int Execute(string sql)
    {
        using var stmt = Prepare(sql);
        while (stmt.Step())
        {
            // drain any rows
        }

        return stmt.RowsAffected;
    }

    /// <summary>Execute SQL and return the first column of the first row, or null.</summary>
    public object? QueryScalar(string sql)
    {
        using var stmt = Prepare(sql);
        return stmt.Step() ? stmt.GetValue(0) : null;
    }

    // ---- extensibility: UDFs / aggregates / collations / load-extension --------------------------

    /// <summary>Register (or, when <paramref name="function"/> is null, unregister) a scalar function.</summary>
    public void RegisterScalarFunction(string name, int argc, bool deterministic, Func<object?[], object?>? function)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(name);
        if (function is null)
        {
            Unregister(name);
            return;
        }

        _extensionHandles.Add(new ScalarFunctionRegistration(name, argc, deterministic, function).Register(_connection));
    }

    /// <summary>Register (or, when <paramref name="step"/> is null, unregister) an aggregate function.</summary>
    public void RegisterAggregateFunction(string name, int argc, object? seed, Func<object?, object?[], object?>? step, Func<object?, object?> finalize)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(name);
        if (step is null)
        {
            Unregister(name);
            return;
        }

        ArgumentNullException.ThrowIfNull(finalize);
        _extensionHandles.Add(new AggregateFunctionRegistration(name, argc, seed, step, finalize).Register(_connection));
    }

    /// <summary>Register (or, when <paramref name="comparison"/> is null, unregister) a collation.</summary>
    public void RegisterCollation(string name, Func<string, string, int>? comparison)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(name);
        if (comparison is null)
        {
            var status = TursoNative.UnregisterCollation(_connection, name, out var errorPtr);
            TursoSyncDatabase.Check(status, errorPtr, "unregister_collation");
            return;
        }

        _extensionHandles.Add(new CollationRegistration(name, comparison).Register(_connection));
    }

    /// <summary>Enable or disable loadable extensions on this connection.</summary>
    public void EnableLoadExtension(bool enabled)
    {
        ThrowIfDisposed();
        var status = TursoNative.EnableLoadExtension(_connection, enabled, out var errorPtr);
        TursoSyncDatabase.Check(status, errorPtr, "enable_load_extension");
    }

    /// <summary>Load a SQLite extension from <paramref name="path"/>.</summary>
    public void LoadExtension(string path)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(path);
        var status = TursoNative.LoadExtension(_connection, path, out var errorPtr);
        TursoSyncDatabase.Check(status, errorPtr, "load_extension");
    }

    private void Unregister(string name)
    {
        var status = TursoNative.UnregisterFunction(_connection, name, out var errorPtr);
        TursoSyncDatabase.Check(status, errorPtr, "unregister_function");
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        if (_connection != IntPtr.Zero)
        {
            TursoNative.ConnectionClose(_connection, out var errorPtr);
            if (errorPtr != IntPtr.Zero)
            {
                TursoNative.FreeString(errorPtr);
            }

            TursoNative.ConnectionDeinit(_connection);
            _connection = IntPtr.Zero;
        }

        // A base (local) connection owns its database handle; a synced connection's database is owned and
        // disposed by its TursoSyncDatabase.
        if (_baseDatabase != IntPtr.Zero)
        {
            TursoNative.DatabaseDeinit(_baseDatabase);
            _baseDatabase = IntPtr.Zero;
        }

        // Free UDF/aggregate/collation context handles after the connection is gone (native destructors
        // have already run; aggregate invocation handles are freed by their native destructor or here).
        foreach (var handle in _extensionHandles)
        {
            if (handle.Target is AggregateFunctionRegistration aggregate)
            {
                aggregate.FreeInvocations();
            }

            if (handle.IsAllocated)
            {
                handle.Free();
            }
        }

        _extensionHandles.Clear();
    }

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, this);
}

/// <summary>
/// The apply gates (<see cref="TursoSyncDatabase.ApplyGate"/>) this thread holds for a native prepare or step in
/// progress, so that a user function, aggregate or collation the engine runs inside that call can let them go.
///
/// <para>Held across a callback, a gate could deadlock: the callback calls anything that takes the database's
/// operation lock (opening a connection, <c>Push</c>, <c>Stats</c>, <c>Pull</c>) while another thread's
/// <c>Pull</c> holds that lock and waits for this thread's read lock to apply. And a callback calling <c>Pull</c>
/// itself would ask for the write lock while holding a read lock, which <see cref="ReaderWriterLockSlim"/> refuses.
/// So user code always runs holding none (<see cref="Suspend"/>), and the gates are taken back when it returns. While
/// the callback runs, an apply may go ahead under the statement that called it, as it always could before the
/// gate.</para>
/// </summary>
internal static class ApplyGateHolds
{
    [ThreadStatic] private static List<ReaderWriterLockSlim>? t_held;

    /// <summary>Take <paramref name="gate"/> shared for one native call (nothing for a local connection).</summary>
    internal static void Enter(ReaderWriterLockSlim? gate)
    {
        if (gate is null)
        {
            return;
        }

        gate.EnterReadLock();
        (t_held ??= []).Add(gate);
    }

    /// <summary>Let go of the <paramref name="gate"/> the matching <see cref="Enter"/> took.</summary>
    internal static void Exit(ReaderWriterLockSlim? gate)
    {
        if (gate is null)
        {
            return;
        }

        t_held!.RemoveAt(t_held.Count - 1);
        gate.ExitReadLock();
    }

    /// <summary>Release every gate this thread holds until the returned scope is disposed, then take them back in
    /// the order they were taken. A statement the callback runs takes its own.</summary>
    internal static Suspended Suspend()
    {
        var held = t_held;
        if (held is null || held.Count == 0)
        {
            return default;
        }

        t_held = null;
        for (var i = held.Count - 1; i >= 0; i--)
        {
            held[i].ExitReadLock();
        }

        return new Suspended(held);
    }

    internal readonly struct Suspended(List<ReaderWriterLockSlim>? held) : IDisposable
    {
        public void Dispose()
        {
            if (held is null)
            {
                return;
            }

            foreach (var gate in held)
            {
                gate.EnterReadLock();
            }

            t_held = held;
        }
    }
}

/// <summary>A prepared statement. Binding is 1-based; column access is 0-based.</summary>
public sealed class TursoRawStatement : IDisposable
{
    private readonly Action? _pump;
    private readonly ReaderWriterLockSlim? _applyGate;
    private IntPtr _statement;
    private bool _disposed;

    internal TursoRawStatement(IntPtr statement, Action? pump, ReaderWriterLockSlim? applyGate = null)
    {
        _statement = statement;
        _pump = pump;
        _applyGate = applyGate;
    }

    /// <summary>The statement's own IO after a step answered <c>Io</c>: the step's continuation, so it takes turns
    /// with a pull's apply exactly as the step does. The sync pump before it holds nothing.</summary>
    private TursoStatus RunIoOnce(out IntPtr errorPtr)
    {
        ApplyGateHolds.Enter(_applyGate);
        try
        {
            return TursoNative.StatementRunIo(_statement, out errorPtr);
        }
        finally
        {
            ApplyGateHolds.Exit(_applyGate);
        }
    }

    /// <summary>One native step, taking turns with a pull's apply (<see cref="TursoSyncDatabase.ApplyGate"/>).</summary>
    private TursoStatus StepOnce(out IntPtr errorPtr)
    {
        ApplyGateHolds.Enter(_applyGate);
        try
        {
            return TursoNative.StatementStep(_statement, out errorPtr);
        }
        finally
        {
            ApplyGateHolds.Exit(_applyGate);
        }
    }

    /// <summary>Advance one row. Returns true on a row, false when the statement is done.</summary>
    public bool Step()
    {
        ThrowIfDisposed();
        while (true)
        {
            var status = StepOnce(out var errorPtr);
            switch (status)
            {
                case TursoStatus.Row:
                    FreeIf(errorPtr);
                    return true;
                case TursoStatus.Done:
                    FreeIf(errorPtr);
                    return false;
                case TursoStatus.Io:
                    FreeIf(errorPtr);
                    _pump?.Invoke();                          // sync pump first (gated: null for local-only)…
                    var io = RunIoOnce(out var ioErr);
                    TursoSyncDatabase.Check(io, ioErr, "statement_run_io"); // …then local statement IO
                    continue;
                default:
                    // Contention surfaces to the caller with TursoException.IsBusy set; see the note in
                    // TursoRawConnection on why it is deliberately not waited out here.
                    TursoSyncDatabase.Check(status, errorPtr, "statement_step");
                    return false; // unreachable
            }
        }
    }

    /// <summary>Reset the statement for re-execution.</summary>
    public void Reset()
    {
        ThrowIfDisposed();
        var status = TursoNative.StatementReset(_statement, out var errorPtr);
        TursoSyncDatabase.Check(status, errorPtr, "statement_reset");
    }

    /// <summary>Rows affected by the last execution.</summary>
    public int RowsAffected => checked((int)TursoNative.StatementRowsAffected(_statement));

    /// <summary>Result column count.</summary>
    public int ColumnCount => checked((int)TursoNative.StatementColumnCount(_statement));

    /// <summary>Number of bind parameters.</summary>
    public int ParameterCount => checked((int)TursoNative.StatementParameterCount(_statement));

    /// <summary>Name of result column <paramref name="ordinal"/> (0-based).</summary>
    public string ColumnName(int ordinal)
    {
        var ptr = TursoNative.StatementColumnName(_statement, (nuint)ordinal);
        if (ptr == IntPtr.Zero)
        {
            return string.Empty;
        }

        try
        {
            return Marshal.PtrToStringUTF8(ptr) ?? string.Empty;
        }
        finally
        {
            TursoNative.FreeString(ptr);
        }
    }

    /// <summary>Native value kind of column <paramref name="ordinal"/>.</summary>
    internal TursoValueKind ValueKind(int ordinal) => TursoNative.StatementRowValueKind(_statement, (nuint)ordinal);

    /// <summary>Declared SQLite type of column <paramref name="ordinal"/> (e.g. "TEXT"), or empty.</summary>
    public string Decltype(int ordinal)
    {
        var ptr = TursoNative.StatementColumnDecltype(_statement, (nuint)ordinal);
        if (ptr == IntPtr.Zero)
        {
            return string.Empty;
        }

        try
        {
            return Marshal.PtrToStringUTF8(ptr) ?? string.Empty;
        }
        finally
        {
            TursoNative.FreeString(ptr);
        }
    }

    /// <summary>Declared name (with sigil) of the 1-based bind parameter at <paramref name="position"/>.</summary>
    public string ParameterName(int position)
    {
        var ptr = TursoNative.StatementParameterName(_statement, position);
        if (ptr == IntPtr.Zero)
        {
            return string.Empty;
        }

        try
        {
            return Marshal.PtrToStringUTF8(ptr) ?? string.Empty;
        }
        finally
        {
            TursoNative.FreeString(ptr);
        }
    }

    /// <summary>1-based position of named parameter <paramref name="name"/>, or 0 if not found.</summary>
    public int NamedPosition(string name)
    {
        var index = TursoNative.StatementNamedPosition(_statement, name);
        return index < 1 ? 0 : checked((int)index);
    }

    /// <summary>Bind a CLR value at 1-based <paramref name="position"/>.</summary>
    public void Bind(int position, object? value)
    {
        ThrowIfDisposed();
        var pos = (nuint)position;
        TursoStatus status = value switch
        {
            null or DBNull => TursoNative.StatementBindNull(_statement, pos),
            bool b => TursoNative.StatementBindInt(_statement, pos, b ? 1 : 0),
            int i => TursoNative.StatementBindInt(_statement, pos, i),
            long l => TursoNative.StatementBindInt(_statement, pos, l),
            short s => TursoNative.StatementBindInt(_statement, pos, s),
            byte bt => TursoNative.StatementBindInt(_statement, pos, bt),
            double d => TursoNative.StatementBindDouble(_statement, pos, d),
            float f => TursoNative.StatementBindDouble(_statement, pos, f),
            byte[] blob => BindBytes(pos, blob, isText: false),
            string str => BindBytes(pos, Encoding.UTF8.GetBytes(str), isText: true),
            _ => BindBytes(pos, Encoding.UTF8.GetBytes(Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture) ?? ""), isText: true),
        };
        TursoSyncDatabase.Check(status, IntPtr.Zero, "bind");
    }

    /// <summary>Read column <paramref name="ordinal"/> as a CLR value.</summary>
    public object? GetValue(int ordinal)
    {
        var index = (nuint)ordinal;
        return TursoNative.StatementRowValueKind(_statement, index) switch
        {
            TursoValueKind.Null or TursoValueKind.Unknown => null,
            TursoValueKind.Integer => TursoNative.StatementRowValueInt(_statement, index),
            TursoValueKind.Real => TursoNative.StatementRowValueDouble(_statement, index),
            TursoValueKind.Text => Encoding.UTF8.GetString(ReadBytes(index)),
            TursoValueKind.Blob => ReadBytes(index),
            _ => null,
        };
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        if (_statement != IntPtr.Zero)
        {
            TursoNative.StatementFinalize(_statement, out var errorPtr);
            if (errorPtr != IntPtr.Zero)
            {
                TursoNative.FreeString(errorPtr);
            }

            TursoNative.StatementDeinit(_statement);
            _statement = IntPtr.Zero;
        }
    }

    private TursoStatus BindBytes(nuint position, byte[] bytes, bool isText)
    {
        if (bytes.Length == 0)
        {
            return isText
                ? TursoNative.StatementBindText(_statement, position, IntPtr.Zero, 0)
                : TursoNative.StatementBindBlob(_statement, position, IntPtr.Zero, 0);
        }

        var handle = GCHandle.Alloc(bytes, GCHandleType.Pinned);
        try
        {
            var ptr = handle.AddrOfPinnedObject();
            var len = (nuint)bytes.Length;
            return isText
                ? TursoNative.StatementBindText(_statement, position, ptr, len)
                : TursoNative.StatementBindBlob(_statement, position, ptr, len);
        }
        finally
        {
            handle.Free();
        }
    }

    private byte[] ReadBytes(nuint index)
    {
        var length = TursoNative.StatementRowValueBytesCount(_statement, index);
        if (length <= 0)
        {
            return [];
        }

        var ptr = TursoNative.StatementRowValueBytesPtr(_statement, index);
        if (ptr == IntPtr.Zero)
        {
            return [];
        }

        var dst = new byte[length];
        Marshal.Copy(ptr, dst, 0, checked((int)length));
        return dst;
    }

    private static void FreeIf(IntPtr errorPtr)
    {
        if (errorPtr != IntPtr.Zero)
        {
            TursoNative.FreeString(errorPtr);
        }
    }

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, this);
}
