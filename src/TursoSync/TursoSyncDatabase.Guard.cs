namespace Turso.Sync;

public sealed partial class TursoSyncDatabase
{
    /// <summary>The engines of this process that are open, by the full path of their replica: what the schema guard
    /// snapshots through when it cannot copy a replica's files (<see cref="GuardScope.BeforeCreate"/>).</summary>
    private static readonly Dictionary<string, List<TursoSyncDatabase>> Live = new(PathComparer);

    private static StringComparer PathComparer =>
        OperatingSystem.IsWindows() || OperatingSystem.IsMacOS() ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal;

    private static string LiveKey(string path) => Path.GetFullPath(path);

    private static void AddLive(TursoSyncDatabase database, string path)
    {
        lock (Live)
        {
            var key = LiveKey(path);
            if (!Live.TryGetValue(key, out var engines))
            {
                Live[key] = engines = [];
            }

            engines.Add(database);
        }
    }

    private static void RemoveLive(TursoSyncDatabase database)
    {
        if (database._config is not { } config)
        {
            return;
        }

        lock (Live)
        {
            var key = LiveKey(config.Path);
            if (Live.TryGetValue(key, out var engines) && engines.Remove(database) && engines.Count == 0)
            {
                Live.Remove(key);
            }
        }
    }

    private static List<TursoSyncDatabase> LiveOn(string path)
    {
        lock (Live)
        {
            return Live.TryGetValue(LiveKey(path), out var engines) ? [.. engines] : [];
        }
    }

    /// <summary>
    /// One schema-guard window around a state-rewriting sync operation: the pre-operation user-table list
    /// (+ optionally a file backup), verified against the post-operation list. See
    /// <see cref="TursoSchemaGuardMode"/>. Null scope = guard inactive (Off, local-only, or nothing on disk
    /// to protect).
    /// </summary>
    private sealed class GuardScope
    {
        private readonly List<string> _before;
        private readonly string _mainPath;
        // A pre-operation copy of the replica files: kept for DetectAndBackup; for Detect (create) it is
        // provisional — promoted to a kept backup when a drop is detected, deleted otherwise. Null when no
        // copy was taken (Detect around a pull: the files are live-open, the table list is the guard).
        private readonly string? _copyDir;
        private readonly bool _copyIsProvisional;

        private GuardScope(List<string> before, string mainPath, string? copyDir, bool copyIsProvisional)
        {
            _before = before;
            _mainPath = mainPath;
            _copyDir = copyDir;
            _copyIsProvisional = copyIsProvisional;
        }

        /// <summary>Guard window for create/open of an existing replica bound to a remote.</summary>
        internal static GuardScope? BeforeCreate(TursoSyncConfig config)
        {
            if (config.SchemaGuard == TursoSchemaGuardMode.Off
                || string.IsNullOrEmpty(config.RemoteUrl)
                || !File.Exists(config.Path))
            {
                return null;
            }

            // The copy sits beside the database (same volume → the promote rename below cannot fail on a
            // cross-volume move). Provisional for Detect; already the kept backup for DetectAndBackup.
            var keep = config.SchemaGuard == TursoSchemaGuardMode.DetectAndBackup;
            var copyDir = TursoSchemaGuard.BackupDirFor(config.Path, DateTime.UtcNow) + (keep ? "" : "-pending");
            var copy = Path.Combine(copyDir, Path.GetFileName(config.Path));

            // A live engine in this process holds the replica, and the files cannot be copied: the engine's exclusive
            // file lock refuses .NET's copy on macOS (a shared advisory lock) and on Windows (LockFileEx over the
            // whole file), and a copy of files an engine is writing could be torn wherever it is allowed. So the
            // pre-image is a snapshot taken through that engine instead.
            if (!TrySnapshotThroughLiveEngine(config.Path, copy))
            {
                TursoSchemaGuard.CopyReplicaFiles(config.Path, copyDir);
            }

            var before = TursoSchemaGuard.UserTablesOfCopy(copy);
            return new GuardScope(before, config.Path, copyDir, copyIsProvisional: !keep);
        }

        /// <summary>Guard window for applying a pulled change-set on <paramref name="database"/>.</summary>
        internal static GuardScope? BeforeApply(TursoSyncDatabase database)
        {
            var config = database._config;
            if (config is null
                || config.SchemaGuard == TursoSchemaGuardMode.Off
                || string.IsNullOrEmpty(config.RemoteUrl))
            {
                return null;
            }

            List<string> before;
            string? copyDir = null;
            using (var connection = TursoRawConnection.Open(database))
            {
                before = TursoSchemaGuard.UserTables(connection);

                // This engine has the replica open, so its files cannot be copied (see BeforeCreate): the backup
                // is a snapshot taken through it.
                if (config.SchemaGuard == TursoSchemaGuardMode.DetectAndBackup)
                {
                    copyDir = TursoSchemaGuard.BackupDirFor(config.Path, DateTime.UtcNow);
                    TursoSchemaGuard.SnapshotInto(connection, Path.Combine(copyDir, Path.GetFileName(config.Path)));
                }
            }

            return new GuardScope(before, config.Path, copyDir, copyIsProvisional: false);
        }

        /// <summary>Snapshot the replica at <paramref name="path"/> into <paramref name="target"/> through a live engine
        /// of this process that has it open; false when none does, or the one found was disposed meanwhile.</summary>
        private static bool TrySnapshotThroughLiveEngine(string path, string target)
        {
            foreach (var engine in LiveOn(path))
            {
                try
                {
                    using var connection = TursoRawConnection.Open(engine);
                    TursoSchemaGuard.SnapshotInto(connection, target);
                    return true;
                }
                catch (ObjectDisposedException)
                {
                    // Closed between the lookup and the open: try the next, or copy the files once none is left.
                }
            }

            return false;
        }

        /// <summary>Compare against the post-operation table list; throw on a drop, clean up otherwise.</summary>
        internal void VerifyAfter(TursoSyncDatabase database, string operation)
        {
            List<string> after;
            using (var connection = TursoRawConnection.Open(database))
            {
                after = TursoSchemaGuard.UserTables(connection);
            }

            var dropped = TursoSchemaGuard.Dropped(_before, after);
            if (dropped.Count == 0)
            {
                if (_copyIsProvisional && _copyDir is not null)
                {
                    try { Directory.Delete(_copyDir, recursive: true); } catch { /* best-effort cleanup */ }
                }

                return;
            }

            var backupDir = _copyDir;
            if (_copyIsProvisional && backupDir is not null)
            {
                // Promote the provisional copy to a kept backup — it is now the only pre-drop image.
                var kept = backupDir[..^"-pending".Length];
                try
                {
                    Directory.Move(backupDir, kept);
                    backupDir = kept;
                }
                catch
                {
                    // Keep the pending dir under its provisional name rather than lose the image.
                }
            }

            throw new TursoSchemaGuardException(operation, dropped, backupDir);
        }
    }
}
