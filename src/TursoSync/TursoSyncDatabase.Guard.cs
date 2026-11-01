namespace Turso.Sync;

public sealed partial class TursoSyncDatabase
{
    /// <summary>The engines of this process that are open, by the full path of their replica: what the schema guard
    /// snapshots through when it cannot copy a replica's files (<see cref="GuardScope.BeforeCreate"/>).</summary>
    private static readonly Dictionary<string, List<TursoSyncDatabase>> Live = new(PathComparer);

    private static StringComparer PathComparer =>
        OperatingSystem.IsWindows() || OperatingSystem.IsMacOS() ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal;

    private static string LiveKey(string path) => Path.GetFullPath(path);

    // Under Live's lock. The key is the one the engine was registered under, computed once: a relative path
    // recomputed at Dispose would resolve against whatever the working directory is by then. A borrow is another
    // caller's snapshot running through this engine (TrySnapshotThroughLiveEngine); while one is out, Dispose leaves
    // the native handle to the last borrower to free (_releasePending), rather than freeing it under them.
    private string? _liveKey;
    private int _borrows;
    private bool _releasePending;

    /// <summary>Runs inside a borrow, with the snapshot's connection open on the borrowed engine (tests): how a
    /// test disposes the engine's owner while another caller is using it.</summary>
    internal static Action<TursoSyncDatabase>? SnapshotConnectionOpenedForTest { get; set; }

    /// <summary>How many live engines the registry holds for <paramref name="path"/> (tests).</summary>
    internal static int LiveCountForTest(string path) => LiveOn(path).Count;

    /// <summary>Whether this engine still holds its native handle (tests).</summary>
    internal bool HoldsNativeForTest => _db != IntPtr.Zero;

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
            database._liveKey = key;
        }
    }

    /// <summary>Take this engine out of the registry, so no new borrow can find it. True when the caller is to free
    /// its native handle now; false when a borrow is out, and the last one returned will.</summary>
    private bool Retire()
    {
        lock (Live)
        {
            if (_liveKey is { } key)
            {
                if (Live.TryGetValue(key, out var engines) && engines.Remove(this) && engines.Count == 0)
                {
                    Live.Remove(key);
                }

                _liveKey = null;
            }

            if (_borrows > 0)
            {
                _releasePending = true;
                return false;
            }

            return true;
        }
    }

    /// <summary>Borrow <paramref name="engine"/> for another caller's snapshot: false once it has been retired.</summary>
    private static bool TryBorrow(TursoSyncDatabase engine)
    {
        lock (Live)
        {
            if (engine._liveKey is null)
            {
                return false;
            }

            engine._borrows++;
            return true;
        }
    }

    /// <summary>Give a borrow back, freeing the native handle if the owner disposed the engine meanwhile.</summary>
    private static void Return(TursoSyncDatabase engine)
    {
        bool release;
        lock (Live)
        {
            engine._borrows--;
            release = engine._borrows == 0 && engine._releasePending;
        }

        if (release)
        {
            engine.ReleaseNative();
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
                // Borrowed, so its owner disposing it cannot free its native handle under the snapshot: the handle is
                // freed when the borrow is returned (Retire).
                if (!TryBorrow(engine))
                {
                    continue;
                }

                try
                {
                    using var connection = TursoRawConnection.Open(engine);
                    SnapshotConnectionOpenedForTest?.Invoke(engine);
                    TursoSchemaGuard.SnapshotInto(connection, target);
                    return true;
                }
                catch (ObjectDisposedException)
                {
                    // Disposed by its owner after the lookup: try the next, or copy the files once none is left (by
                    // then the returned borrow has freed this one's handle and its lock).
                }
                finally
                {
                    Return(engine);
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
