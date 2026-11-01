namespace Turso.Sync;

/// <summary>
/// How <see cref="TursoSyncDatabase"/> protects local schema across the sync operations that can rewrite
/// local state (create/open of an existing replica, and applying pulled changes). The sync engine reconciles
/// the local file against the server's state, and a table the server never learned (for example one created
/// before the remote was attached) can be silently dropped in that reconciliation — see
/// <see cref="TursoSyncDatabase.ReconcileLocalTables"/> for the repair.
/// </summary>
public enum TursoSchemaGuardMode
{
    /// <summary>No protection — the pre-guard behavior.</summary>
    Off,

    /// <summary>
    /// Detect: snapshot the user-table list around the operation and throw
    /// <see cref="TursoSchemaGuardException"/> if tables disappeared. On create/open of an existing replica
    /// the pre-image lives in a temporary copy which is PROMOTED to a kept backup when a drop is detected
    /// (and deleted otherwise); around a pull there is no pre-image, so detection alone. The default.
    /// </summary>
    Detect,

    /// <summary>
    /// Detect, and always keep a timestamped backup of the replica files taken before the operation,
    /// whether or not anything was dropped. The backup directory is <c>&lt;path&gt;.guard-&lt;utc-stamp&gt;</c>
    /// beside the database. When an engine of this process has the replica open (always, around a pull), its
    /// files cannot be copied, and the backup is instead a consistent snapshot of the database taken through that
    /// engine (<c>VACUUM INTO</c>): the main file alone, without the sync sidecars.
    /// </summary>
    DetectAndBackup,
}

/// <summary>
/// Thrown when a sync operation dropped local tables (see <see cref="TursoSchemaGuardMode"/>). The drop has
/// already happened by the time this is thrown — the guard cannot veto the engine — but
/// <see cref="BackupDirectory"/>, when non-null, holds a pre-operation copy of the replica files from which
/// the tables can be recovered, and <see cref="TursoSyncDatabase.ReconcileLocalTables"/> can teach the server
/// tables it never learned so the drop does not recur.
/// </summary>
public sealed class TursoSchemaGuardException : Exception
{
    /// <summary>Create the exception.</summary>
    public TursoSchemaGuardException(string operation, IReadOnlyList<string> droppedTables, string? backupDirectory)
        : base(BuildMessage(operation, droppedTables, backupDirectory))
    {
        Operation = operation;
        DroppedTables = droppedTables;
        BackupDirectory = backupDirectory;
    }

    /// <summary>The sync operation that dropped the tables (<c>create</c> or <c>pull</c>).</summary>
    public string Operation { get; }

    /// <summary>The user tables that existed before the operation and are gone after it.</summary>
    public IReadOnlyList<string> DroppedTables { get; }

    /// <summary>Directory holding a pre-operation copy of the replica files, or a snapshot of the database when the
    /// replica was open (see <see cref="TursoSchemaGuardMode.DetectAndBackup"/>); null when none was taken
    /// (<see cref="TursoSchemaGuardMode.Detect"/> around a pull).</summary>
    public string? BackupDirectory { get; }

    private static string BuildMessage(string operation, IReadOnlyList<string> dropped, string? backupDir) =>
        $"Sync {operation} dropped local table(s): {string.Join(", ", dropped)}." +
        (backupDir is null
            ? " No pre-operation backup was taken (SchemaGuard=Detect around a pull)."
            : $" A pre-operation backup of the replica was kept at '{backupDir}'.") +
        " If these tables were created before the remote was attached, ReconcileLocalTables() teaches the" +
        " server their schema and rows so the drop does not recur.";
}

/// <summary>Table-inventory + replica-file-backup helpers shared by the guard and reconcile.</summary>
internal static class TursoSchemaGuard
{
    /// <summary>Sidecar suffixes that make up a replica on disk, alongside the main file itself.</summary>
    internal static readonly string[] ReplicaSuffixes = ["", "-wal", "-shm", "-info", "-changes", "-wal-revert"];

    /// <summary>
    /// The user tables visible on <paramref name="connection"/> — everything in <c>sqlite_master</c> except
    /// SQLite's own tables and the engine's <c>turso_*</c>/<c>__turso*</c> bookkeeping.
    /// </summary>
    internal static List<string> UserTables(TursoRawConnection connection)
    {
        var tables = new List<string>();
        using var stmt = connection.Prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name");
        while (stmt.Step())
        {
            if (stmt.GetValue(0) is string name && !IsInternal(name))
            {
                tables.Add(name);
            }
        }

        return tables;
    }

    internal static bool IsInternal(string table) =>
        table.StartsWith("sqlite_", StringComparison.OrdinalIgnoreCase) ||
        table.StartsWith("turso_", StringComparison.OrdinalIgnoreCase) ||
        table.StartsWith("__turso", StringComparison.OrdinalIgnoreCase);

    /// <summary>Tables present in <paramref name="before"/> but absent from <paramref name="after"/>.</summary>
    internal static List<string> Dropped(IReadOnlyCollection<string> before, IReadOnlyCollection<string> after)
    {
        var kept = new HashSet<string>(after, StringComparer.OrdinalIgnoreCase);
        return before.Where(t => !kept.Contains(t)).ToList();
    }

    /// <summary>
    /// Copy the replica's on-disk files (main + sidecars that exist) into <paramref name="targetDir"/>,
    /// creating it. Plain file copies — never opens the database.
    /// </summary>
    internal static void CopyReplicaFiles(string mainPath, string targetDir)
    {
        Directory.CreateDirectory(targetDir);
        foreach (var suffix in ReplicaSuffixes)
        {
            var source = mainPath + suffix;
            if (File.Exists(source))
            {
                File.Copy(source, Path.Combine(targetDir, Path.GetFileName(source)), overwrite: true);
            }
        }
    }

    /// <summary>
    /// Write a consistent snapshot of the database <paramref name="connection"/> is open on to
    /// <paramref name="target"/> (<c>VACUUM INTO</c>), creating its directory. For a replica an engine holds,
    /// whose files cannot be copied (<see cref="CopyReplicaFiles"/>).
    /// </summary>
    internal static void SnapshotInto(TursoRawConnection connection, string target)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(target)!);
        connection.Execute("VACUUM INTO '" + target.Replace("'", "''", StringComparison.Ordinal) + "'");
    }

    /// <summary>The kept-backup directory name for <paramref name="mainPath"/> at <paramref name="utcNow"/>.</summary>
    internal static string BackupDirFor(string mainPath, DateTime utcNow) =>
        mainPath + ".guard-" + utcNow.ToString("yyyyMMdd-HHmmss-fff", System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>
    /// Read the user-table list of an offline COPY of a replica (never the live file — opening a
    /// sync-managed file with the base engine disturbs its WAL bookkeeping). The copy is opened with the
    /// plain local engine, which applies the copied WAL for a consistent read.
    /// </summary>
    internal static List<string> UserTablesOfCopy(string copiedMainPath)
    {
        using var connection = TursoRawConnection.OpenLocal(new TursoSyncConfig { Path = copiedMainPath });
        return UserTables(connection);
    }
}
