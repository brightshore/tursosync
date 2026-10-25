namespace Turso.Sync;

/// <summary>Public projection of Turso sync-engine statistics.</summary>
/// <param name="CdcOperations">Local operations written since the last pull.</param>
/// <param name="MainWalSize">Size of the main WAL file.</param>
/// <param name="RevertWalSize">Size of the revert WAL file.</param>
/// <param name="LastPullUnixTime">Unix time of the last successful pull.</param>
/// <param name="LastPushUnixTime">Unix time of the last successful push.</param>
/// <param name="NetworkSentBytes">Total bytes sent over the network.</param>
/// <param name="NetworkReceivedBytes">Total bytes received over the network.</param>
/// <param name="Revision">Opaque server revision string.</param>
public sealed record TursoStats(
    long CdcOperations,
    long MainWalSize,
    long RevertWalSize,
    long LastPullUnixTime,
    long LastPushUnixTime,
    long NetworkSentBytes,
    long NetworkReceivedBytes,
    string Revision);

/// <summary>
/// What kind of failure a <see cref="TursoException"/> represents, so callers can branch on the engine's own
/// classification instead of matching on message text.
/// </summary>
public enum TursoErrorKind
{
    /// <summary>Not one of the categories below — inspect <see cref="Exception.Message"/>.</summary>
    Unknown = 0,

    /// <summary>Lock contention; the same operation is worth retrying. See <see cref="TursoException.IsBusy"/>.</summary>
    Busy,

    /// <summary>The read snapshot went stale; the transaction must be restarted, not retried.
    /// See <see cref="TursoException.IsBusySnapshot"/>.</summary>
    BusySnapshot,

    /// <summary>The statement was interrupted.</summary>
    Interrupted,

    /// <summary>A constraint (unique, check, not-null, foreign key) was violated — a data error, not a fault.</summary>
    Constraint,

    /// <summary>The database is read-only.</summary>
    ReadOnly,

    /// <summary>The database is full.</summary>
    DatabaseFull,

    /// <summary>The file is not a database.</summary>
    NotADatabase,

    /// <summary>The database is corrupt.</summary>
    Corrupt,

    /// <summary>An IO failure.</summary>
    Io,

    /// <summary>The API was used incorrectly (a bug in the caller).</summary>
    Misuse,
}

/// <summary>An error surfaced from the Turso native sync/SQL engine.</summary>
public class TursoException : Exception
{
    /// <summary>Create a <see cref="TursoException"/> with a message.</summary>
    public TursoException(string message) : base(message)
    {
    }

    /// <summary>Create a <see cref="TursoException"/> with a message and inner exception.</summary>
    public TursoException(string message, Exception inner) : base(message, inner)
    {
    }

    internal TursoException(string message, TursoStatus status) : base(message)
    {
        Kind = Classify(message, status);
    }

    /// <summary>The engine's classification of this failure.</summary>
    public TursoErrorKind Kind { get; }

    /// <summary>
    /// Map the native status to a public kind. Contention is also matched on message text because the engine
    /// sometimes reports it with a generic status plus an explanatory string rather than the Busy code.
    /// </summary>
    private static TursoErrorKind Classify(string message, TursoStatus status) => status switch
    {
        TursoStatus.BusySnapshot => TursoErrorKind.BusySnapshot,
        TursoStatus.Busy => TursoErrorKind.Busy,
        TursoStatus.Interrupt => TursoErrorKind.Interrupted,
        TursoStatus.Constraint => TursoErrorKind.Constraint,
        TursoStatus.Readonly => TursoErrorKind.ReadOnly,
        TursoStatus.DatabaseFull => TursoErrorKind.DatabaseFull,
        TursoStatus.NotADatabase => TursoErrorKind.NotADatabase,
        TursoStatus.Corrupt => TursoErrorKind.Corrupt,
        TursoStatus.Io or TursoStatus.IoError => TursoErrorKind.Io,
        TursoStatus.Misuse => TursoErrorKind.Misuse,
        _ when message.Contains("snapshot is stale", StringComparison.OrdinalIgnoreCase)
            => TursoErrorKind.BusySnapshot,
        _ when message.Contains("database is locked", StringComparison.OrdinalIgnoreCase)
            || message.Contains("database is busy", StringComparison.OrdinalIgnoreCase)
            => TursoErrorKind.Busy,
        _ => TursoErrorKind.Unknown,
    };

    /// <summary>
    /// True when this is plain lock contention — the caller lost a race for a lock and the same operation is
    /// worth retrying once the lock frees.
    ///
    /// <para>Note the engine's busy handler covers <c>step</c> only: on a busy step it yields IO until the
    /// connection's busy timeout elapses, which the statement loop drives. It is NOT consulted while
    /// preparing, so <see cref="TursoRawConnection.Prepare"/> gives a contended <c>prepare</c> the same wait
    /// itself while the connection is in autocommit, and this surfaces only once the busy timeout has run out.
    /// Inside a transaction a contended <c>prepare</c> still surfaces here immediately, whatever the timeout
    /// (measured in <c>TursoBusyTimeoutTests</c>).</para>
    /// </summary>
    public bool IsBusy => Kind is TursoErrorKind.Busy;

    /// <summary>
    /// True when the transaction's read snapshot went stale — typically because a checkpoint advanced the WAL
    /// underneath it. Mutually exclusive with <see cref="IsBusy"/>, and the distinction matters: the engine is
    /// explicit that "a busy_timeout or handler should not be used because it will not help — the snapshot is
    /// permanently stale and rollback is the only way out for this poor transaction". Retrying the statement
    /// in place spins until it gives up; the transaction has to be rolled back and restarted to get a fresh
    /// snapshot.
    /// </summary>
    public bool IsBusySnapshot => Kind is TursoErrorKind.BusySnapshot;
}

/// <summary>
/// The on-disk sync state next to a synced database (the <c>-info</c> metadata sidecar, or the
/// engine-version stamp) is unusable by this engine — unparseable, an unsupported metadata version, or
/// written by a <b>newer</b> engine than the one now opening it. Thrown BEFORE the native engine touches
/// the files, precisely because the native layer's response to foreign state can be a Rust panic that
/// aborts the whole process (observed 2026-08-08: <c>turso_sync_operation_resume</c> → <c>abort()</c>
/// on state written by a different engine vintage). Catch it and decide policy: quarantine the local
/// files and re-bootstrap from the remote, or surface the refusal. A subclass of
/// <see cref="TursoException"/> so existing catch sites keep working.
/// </summary>
public sealed class TursoSyncStateException : TursoException
{
    /// <summary>Create a <see cref="TursoSyncStateException"/> for the state file at <paramref name="statePath"/>.</summary>
    public TursoSyncStateException(string statePath, string message) : base(message) => StatePath = statePath;

    /// <summary>Full path of the offending state file.</summary>
    public string StatePath { get; }
}
