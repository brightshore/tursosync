using System.Diagnostics;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Text;

namespace Turso.Sync;

/// <summary>
/// A synced Turso database and the host-driven IO loop that powers it. Ported from the Go binding's
/// <c>driver_sync.go</c>: every async engine operation is driven by <see cref="DriveOpUntilDone"/>, which
/// resumes the operation and, whenever the engine asks for IO, executes the requested HTTP / file work and
/// feeds the result back. This provider is the <i>host</i> — the engine never does network or disk itself.
/// </summary>
public sealed partial class TursoSyncDatabase : IDisposable
{
    private readonly object _gate = new();

    /// <summary>The lock every engine operation holds, connect included. Tests hold it to stand in for a push or
    /// pull that is on the network.</summary>
    internal object Gate => _gate;
    /// <summary>
    /// Optional callback for one safe diagnostic per completed HTTP request (including failed requests).
    /// Null by default. Runs synchronously under the database's operation lock; keep it fast and do not
    /// reenter the database. Exceptions from the callback are ignored. Shared by all connections to this
    /// replica; coordinate changes with other users of the database. Captured once per request.
    /// </summary>
    public Action<TursoSyncHttpDiagnostic>? HttpDiagnostics { get; set; }

    private readonly HttpClient _http;
    private readonly string _baseUrl;
    private readonly string? _authToken;
    private readonly string? _namespace;
    private TursoSyncConfig? _config;
    private IntPtr _db;
    private bool _disposed;

    static TursoSyncDatabase() => TursoNativeLibrary.EnsureResolver();

    private TursoSyncDatabase(IntPtr db, string baseUrl, string? authToken, string? ns, HttpClient http)
    {
        _db = db;
        _baseUrl = baseUrl;
        _authToken = authToken;
        _namespace = ns;
        _http = http;
    }

    /// <summary>
    /// Create (or open) a synced database per <paramref name="config"/>, driving the create operation to
    /// completion. Bootstraps from the remote if configured; otherwise opens purely locally.
    /// </summary>
    public static TursoSyncDatabase Create(TursoSyncConfig config)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentOutOfRangeException.ThrowIfNegative(config.PushOperationsThreshold);

        // Local at-rest encryption is a base-engine feature only. The upstream sync engine never plumbs the
        // pager cipher/key into its local files (the Go binding this was ported from has no such config), and
        // a synced DB created with one writes an encrypted page it then cannot reopen ("Decryption failed for
        // page=1"). Reject it loudly here rather than hand back a database that's lost on the next open. For
        // local at-rest encryption use a local-only connection (no Remote Url, Sync disabled), which opens
        // through the base engine. (Turso Cloud's server-side encryption is a separate, remote-key concept.)
        if (config.IsEncrypted)
        {
            throw new NotSupportedException(
                "Local at-rest encryption is not supported on the sync engine. Configure EncryptionCipher/" +
                "EncryptionKey only on a local-only database (no Remote Url and Sync disabled), which opens " +
                "through the base engine.");
        }

        // Refuse foreign/unusable on-disk sync state HERE, with a typed exception, while the files are
        // still untouched — the native engine's own response to it can be a panic that aborts the whole
        // process (see TursoSyncStateGuard).
        TursoSyncStateGuard.Validate(config.Path);

        var baseUrl = NormalizeUrl(config.RemoteUrl ?? "");
        var http = new HttpClient { Timeout = config.EffectiveHttpTimeout };

        // Schema guard, pre-open: snapshot the existing replica's user tables BEFORE the sync engine touches
        // the file — opening an existing replica against a remote can revert/reconcile local state, and a
        // table the server never learned can be dropped in that reconciliation. The snapshot is taken from a
        // temporary COPY of the files (never by opening the live file with the base engine, which would
        // disturb the sync WAL bookkeeping). See TursoSchemaGuardMode.
        var guard = GuardScope.BeforeCreate(config);

        // AsyncIo MUST be true for the sync engine so it hands IO to us instead of doing it itself.
        var (dbConfig, dbConfigPtrs) = TursoConfigMarshal.BuildDatabaseConfig(config, asyncIo: 1);
        var strings = new MarshaledStrings();
        IntPtr db;
        try
        {
            var syncConfig = new TursoSyncDatabaseConfig
            {
                Path = strings.Utf8(config.Path),
                RemoteUrl = strings.Utf8OrNull(baseUrl),
                ClientName = strings.Utf8(config.ClientName),
                LongPollTimeoutMs = config.LongPollTimeoutMs,
                BootstrapIfEmpty = config.BootstrapIfEmpty,
                PushOperationsThreshold = (nuint)config.PushOperationsThreshold,
                PullBytesThreshold = (nuint)Math.Max(0, config.PullBytesThreshold),
            };

            var status = TursoNative.SyncDatabaseNew(ref dbConfig, ref syncConfig, out db, out var errorPtr);
            Check(status, errorPtr, "sync_database_new");
        }
        finally
        {
            strings.Dispose();
            TursoConfigMarshal.Free(dbConfigPtrs);
        }

        var self = new TursoSyncDatabase(db, baseUrl.TrimEnd('/'), Trim(config.AuthToken), config.Namespace, http)
        {
            _config = config,
            HttpDiagnostics = config.HttpDiagnostics,
        };
        try
        {
            var op = self.Begin(TursoNative.SyncDatabaseCreate, "sync_database_create");
            self.DriveAndDeinit(op, "sync_database_create");
            guard?.VerifyAfter(self, "create");
            // The open succeeded — this engine version now owns the on-disk sync state.
            TursoSyncStateGuard.Stamp(config.Path);
            return self;
        }
        catch
        {
            self.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Open a new connection to the synced database, driving the connect operation. The returned handle is
    /// a raw <c>turso_connection_t</c>; the caller owns it and must close/deinit it.
    ///
    /// <para>A contended connect is retried here until <paramref name="busyTimeoutMs"/> runs out. The engine's
    /// connect runs a CDC pragma (and a CDC-version read) on a brand-new connection — through <c>prepare</c>,
    /// which never consults the busy handler, and before any busy timeout has been set on it — so a connect
    /// that landed while a checkpoint held the lock failed outright with <c>database tape error: database is
    /// busy</c> (TursoConnectBusyTests). Unlike the statement retry reverted in TursoRawConnection, each attempt
    /// here is a fresh connection with no snapshot to go stale, so retrying it is sound.</para>
    ///
    /// <para>Time spent waiting for the database's gate is not charged to the busy timeout. A push or pull holds
    /// the gate for its whole round trip, so a connect could wait far longer than its timeout before its first
    /// attempt; when the deadline counted from before that wait, the first lock collision afterwards failed at
    /// once with <c>database is busy</c>. The timeout bounds lock contention, which is what it is for.</para>
    /// </summary>
    public IntPtr Connect(int busyTimeoutMs)
    {
        var timeout = busyTimeoutMs == 0 ? 5000 : busyTimeoutMs;
        var deadline = Clock() + Math.Max(0, timeout);
        var backoffMs = 1;
        while (true)
        {
            long gateWaitMs = 0;
            try
            {
                return ConnectOnce(timeout, ref gateWaitMs);
            }
            catch (TursoException ex) when (ex.IsBusy || ex.IsBusySnapshot)
            {
                deadline += gateWaitMs;

                // Never sleep past the deadline, and never attempt once it has passed: the busy timeout is a
                // bound on the whole connect, not on when the last attempt may start.
                var remaining = deadline - Clock();
                if (remaining <= 0)
                {
                    throw;
                }

                // Outside the gate, so whatever holds the lock (a checkpoint, a push) can finish meanwhile.
                Sleep((int)Math.Min(backoffMs, remaining));
                if (Clock() >= deadline)
                {
                    throw;
                }

                backoffMs = Math.Min(backoffMs * 2, 50);
            }
        }
    }

    /// <summary>The clock <see cref="Connect"/> measures its busy timeout with, in milliseconds. Tests replace it.</summary>
    internal Func<long> Clock { get; set; } = static () => Environment.TickCount64;

    /// <summary>How <see cref="Connect"/> backs off between attempts. Tests replace it along with <see cref="Clock"/>.</summary>
    internal Action<int> Sleep { get; set; } = Thread.Sleep;

    /// <summary>Runs inside the gate just before each native connect attempt. A test seam: throwing a busy
    /// <see cref="TursoException"/> from it stands in for a lock collision.</summary>
    internal Action? ConnectAttemptStarting { get; set; }

    private IntPtr ConnectOnce(int timeout, ref long gateWaitMs)
    {
        var waitStart = Clock();
        lock (_gate)
        {
            gateWaitMs = Clock() - waitStart;
            ThrowIfDisposed();
            ConnectAttemptStarting?.Invoke();
            var op = Begin(TursoNative.SyncDatabaseConnect, "sync_database_connect");
            try
            {
                var kind = DriveOpUntilDone(op, "sync_database_connect");
                if (kind != TursoSyncResultKind.Connection)
                {
                    throw new TursoException($"sync_database_connect: unexpected result kind {kind}");
                }

                var status = TursoNative.SyncOperationExtractConnection(op, out var connection);
                Check(status, IntPtr.Zero, "extract_connection");

                if (timeout > 0)
                {
                    TursoNative.ConnectionSetBusyTimeoutMs(connection, timeout);
                }

                return connection;
            }
            finally
            {
                TursoNative.SyncOperationDeinit(op);
            }
        }
    }

    /// <summary>Push local changes to the remote. No-op effect when local-only.</summary>
    public void Push()
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            DriveAndDeinit(Begin(TursoNative.SyncDatabasePushChanges, "push_changes"), "push_changes");
        }
    }

    /// <summary>
    /// Pull remote changes and apply them locally. Returns true if any changes were applied.
    /// </summary>
    public bool Pull()
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            var waitOp = Begin(TursoNative.SyncDatabaseWaitChanges, "wait_changes");
            IntPtr changes;
            try
            {
                var kind = DriveOpUntilDone(waitOp, "wait_changes");
                if (kind != TursoSyncResultKind.Changes)
                {
                    throw new TursoException($"wait_changes: unexpected result kind {kind}");
                }

                var status = TursoNative.SyncOperationExtractChanges(waitOp, out changes);
                Check(status, IntPtr.Zero, "extract_changes");
            }
            finally
            {
                TursoNative.SyncOperationDeinit(waitOp);
            }

            if (changes == IntPtr.Zero)
            {
                return false;
            }

            // Schema guard, pre-apply: remember the user tables (and, in DetectAndBackup, copy the replica
            // files) so a pulled change-set that drops local tables surfaces loudly. See TursoSchemaGuardMode.
            var guard = GuardScope.BeforeApply(this);

            // apply_changes consumes the changes handle (even on failure) — do not deinit it ourselves.
            var status2 = TursoNative.SyncDatabaseApplyChanges(_db, changes, out var applyOp, out var errorPtr);
            Check(status2, errorPtr, "apply_changes");
            DriveAndDeinit(applyOp, "apply_changes");
            guard?.VerifyAfter(this, "pull");
            return true;
        }
    }

    /// <summary>Checkpoint the local WAL.</summary>
    public void Checkpoint()
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            DriveAndDeinit(Begin(TursoNative.SyncDatabaseCheckpoint, "checkpoint"), "checkpoint");
        }
    }

    /// <summary>Collect sync stats.</summary>
    public TursoStats Stats()
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            var op = Begin(TursoNative.SyncDatabaseStats, "stats");
            try
            {
                var kind = DriveOpUntilDone(op, "stats");
                if (kind != TursoSyncResultKind.Stats)
                {
                    throw new TursoException($"stats: unexpected result kind {kind}");
                }

                var status = TursoNative.SyncOperationExtractStats(op, out var stats);
                Check(status, IntPtr.Zero, "extract_stats");
                return new TursoStats(
                    stats.CdcOperations,
                    stats.MainWalSize,
                    stats.RevertWalSize,
                    stats.LastPullUnixTime,
                    stats.LastPushUnixTime,
                    stats.NetworkSentBytes,
                    stats.NetworkReceivedBytes,
                    SliceToString(stats.Revision));
            }
            finally
            {
                TursoNative.SyncOperationDeinit(op);
            }
        }
    }

    /// <summary>
    /// Process at most one queued IO item, then step engine callbacks. Called once per statement-step
    /// iteration by the ADO.NET layer so queries on a synced DB also advance sync IO (Go's <c>extra</c>).
    /// </summary>
    public void ProcessOneIo()
    {
        ThrowIfDisposed();
        var status = TursoNative.SyncIoTakeItem(_db, out var item, out var errorPtr);
        Check(status, errorPtr, "io_take_item");
        if (item != IntPtr.Zero)
        {
            try
            {
                HandleIoItem(item);
            }
            finally
            {
                TursoNative.SyncIoItemDeinit(item);
            }
        }

        StepCallbacks();
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        if (_db != IntPtr.Zero)
        {
            TursoNative.SyncDatabaseDeinit(_db);
            _db = IntPtr.Zero;
        }

        _http.Dispose();
    }

    // ---- the resume / IO loop --------------------------------------------------------------------

    private delegate TursoStatus BeginOp(IntPtr db, out IntPtr operation, out IntPtr errorPtr);

    private IntPtr Begin(BeginOp begin, string ctx)
    {
        var status = begin(_db, out var op, out var errorPtr);
        Check(status, errorPtr, ctx);
        return op;
    }

    private void DriveAndDeinit(IntPtr op, string ctx)
    {
        try
        {
            DriveOpUntilDone(op, ctx);
        }
        finally
        {
            TursoNative.SyncOperationDeinit(op);
        }
    }

    private TursoSyncResultKind DriveOpUntilDone(IntPtr op, string ctx)
    {
        while (true)
        {
            var code = TursoNative.SyncOperationResume(op, out var errorPtr);
            switch (code)
            {
                case TursoStatus.Done:
                    if (errorPtr != IntPtr.Zero)
                    {
                        TursoNative.FreeString(errorPtr);
                    }

                    return TursoNative.SyncOperationResultKind(op);
                case TursoStatus.Io:
                    if (errorPtr != IntPtr.Zero)
                    {
                        TursoNative.FreeString(errorPtr);
                    }

                    ProcessIoQueue();
                    continue;
                case TursoStatus.Ok:
                    if (errorPtr != IntPtr.Zero)
                    {
                        TursoNative.FreeString(errorPtr);
                    }

                    continue;
                default:
                    Check(code, errorPtr, $"{ctx} resume");
                    continue; // unreachable; Check throws on error codes
            }
        }
    }

    private void ProcessIoQueue()
    {
        while (true)
        {
            var status = TursoNative.SyncIoTakeItem(_db, out var item, out var errorPtr);
            Check(status, errorPtr, "io_take_item");
            if (item == IntPtr.Zero)
            {
                break;
            }

            try
            {
                HandleIoItem(item);
            }
            finally
            {
                TursoNative.SyncIoItemDeinit(item);
            }
        }

        StepCallbacks();
    }

    private void StepCallbacks()
    {
        var status = TursoNative.SyncIoStepCallbacks(_db, out var errorPtr);
        Check(status, errorPtr, "io_step_callbacks");
    }

    private void HandleIoItem(IntPtr item)
    {
        switch (TursoNative.SyncIoRequestKind(item))
        {
            case TursoSyncIoKind.Http:
                HandleHttp(item);
                break;
            case TursoSyncIoKind.FullRead:
                HandleFullRead(item);
                break;
            case TursoSyncIoKind.FullWrite:
                HandleFullWrite(item);
                break;
            default:
                TursoNative.SyncIoDone(item);
                break;
        }
    }

    private void HandleHttp(IntPtr item)
    {
        try
        {
            var status = TursoNative.SyncIoRequestHttp(item, out var req);
            Check(status, IntPtr.Zero, "io_request_http");

            var path = SliceToString(req.Path);

            var headers = new List<KeyValuePair<string, string>>(req.Headers);
            for (var i = 0; i < req.Headers; i++)
            {
                var hs = TursoNative.SyncIoRequestHttpHeader(item, (nuint)i, out var header);
                Check(hs, IntPtr.Zero, "io_request_http_header");
                headers.Add(new KeyValuePair<string, string>(SliceToString(header.Key), SliceToString(header.Value)));
            }

            using var message = BuildHttpMessage(
                SliceToString(req.Method),
                JoinUrl(_baseUrl, path),
                BodyFromSlice(req.Body),
                headers,
                _authToken,
                BuildHost(_baseUrl, _namespace));

            SendHttp(_http, message, HttpDiagnostics,
                statusCode => TursoNative.SyncIoStatus(item, statusCode),
                stream => PumpStream(item, stream));
            TursoNative.SyncIoDone(item);
        }
        catch (Exception ex) when (IsHttpTimeout(ex))
        {
            Poison(item, $"sync request timed out: no response from the server within {_http.Timeout.TotalMilliseconds:0} ms (Http Timeout)");
            TursoNative.SyncIoDone(item);
        }
        catch (Exception ex)
        {
            Poison(item, ex.Message);
            TursoNative.SyncIoDone(item);
        }
    }

    // Keep the transport native-free so loopback tests exercise exactly the path used by HandleHttp.
    internal static void SendHttp(HttpClient http, HttpRequestMessage message,
        Action<TursoSyncHttpDiagnostic>? diagnostics, Action<int> onStatus, Action<Stream> readBody)
    {
        var timer = diagnostics is null ? null : Stopwatch.StartNew();
        TimeSpan? headersDuration = null;
        int? status = null;
        long? declaredBytes = null;
        IdleTimeoutStream? body = null;
        var failure = TursoSyncHttpFailurePhase.BeforeHeaders;
        try
        {
            // ResponseHeadersRead preserves the separate header and per-read idle timeouts.
            using var response = http.Send(message, HttpCompletionOption.ResponseHeadersRead);
            headersDuration = timer?.Elapsed;
            status = (int)response.StatusCode;
            failure = TursoSyncHttpFailurePhase.ResponseBody;
            if (diagnostics is not null)
            {
                declaredBytes = response.Content.Headers.ContentLength;
            }
            onStatus(status.Value);
            using var stream = response.Content.ReadAsStream();
            body = new IdleTimeoutStream(stream, http.Timeout);
            readBody(body);
            failure = TursoSyncHttpFailurePhase.None;
        }
        finally
        {
            if (diagnostics is not null)
            {
                var duration = timer!.Elapsed;
                try
                {
                    diagnostics(new TursoSyncHttpDiagnostic(
                        SafeMethod(message.Method.Method), SafeEndpoint(message.RequestUri),
                        message.Content?.Headers.ContentLength ?? 0, headersDuration, status,
                        declaredBytes, body?.BytesRead, duration, failure));
                }
                catch (Exception)
                {
                    // Observability must never replace a transport error or fail a successful sync.
                }
            }
        }
    }

    private static string SafeMethod(string method) => method switch
    {
        "GET" or "POST" or "PUT" or "DELETE" or "PATCH" or "HEAD" or "OPTIONS" => method,
        _ => "OTHER",
    };

    private static string SafeEndpoint(Uri? uri) => uri?.AbsolutePath switch
    {
        "/info" => "info",
        "/pull-updates" => "pull-updates",
        "/push-updates" => "push-updates",
        _ => "other",
    };

    private static bool IsHttpTimeout(Exception ex) =>
        ex is TimeoutException || (ex is TaskCanceledException && ex.InnerException is TimeoutException);

    /// <summary>A read-only view of a response stream whose every read fails with <see cref="TimeoutException"/> once it
    /// has waited <c>timeout</c> for data.</summary>
    private sealed class IdleTimeoutStream(Stream inner, TimeSpan timeout) : Stream
    {
        public long BytesRead { get; private set; }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var read = ReadCore(buffer, offset, count);
            BytesRead += read;
            return read;
        }

        private int ReadCore(byte[] buffer, int offset, int count)
        {
            if (timeout == Timeout.InfiniteTimeSpan)
            {
                return inner.Read(buffer, offset, count);
            }

            using var cts = new CancellationTokenSource(timeout);
            try
            {
                return inner.ReadAsync(buffer.AsMemory(offset, count), cts.Token).AsTask().GetAwaiter().GetResult();
            }
            catch (OperationCanceledException) when (cts.IsCancellationRequested)
            {
                throw new TimeoutException("the response body stopped arriving");
            }
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    private void HandleFullRead(IntPtr item)
    {
        try
        {
            var status = TursoNative.SyncIoRequestFullRead(item, out var req);
            Check(status, IntPtr.Zero, "io_request_full_read");
            var path = SliceToString(req.Path);

            using var stream = OpenForFullRead(path);
            if (stream is null)
            {
                // A missing file must be treated as an empty file (per the ABI contract).
                TursoNative.SyncIoDone(item);
                return;
            }

            PumpStream(item, stream);
            TursoNative.SyncIoDone(item);
        }
        catch (Exception ex)
        {
            Poison(item, ex.Message);
            TursoNative.SyncIoDone(item);
        }
    }

    private static void HandleFullWrite(IntPtr item)
    {
        try
        {
            var status = TursoNative.SyncIoRequestFullWrite(item, out var req);
            Check(status, IntPtr.Zero, "io_request_full_write");
            WriteFileAtomically(SliceToString(req.Path), SliceToBytes(req.Content) ?? []);
            TursoNative.SyncIoDone(item);
        }
        catch (Exception ex)
        {
            Poison(item, ex.Message);
            TursoNative.SyncIoDone(item);
        }
    }

    /// <summary>
    /// Open a file for the engine's whole-file read, sharing it with writers that may replace it, or null if it
    /// doesn't exist.
    ///
    /// <para>On Windows the open is refused (<c>being used by another process</c>) while
    /// <see cref="WriteFileAtomically"/>'s rename is replacing the file, so a refusal is retried briefly, as the
    /// rename is.</para>
    ///
    /// <para>Windows can also show the file as briefly MISSING mid-replace: its replacing rename is not atomic to a
    /// reader the way <c>rename(2)</c> is (measured: one run in ten of TursoAtomicWriteTests). The engine reads a
    /// missing file as empty, which for <c>-info</c> would mean no sync metadata, so on Windows a missing file is
    /// looked for again for <see cref="MissingRecheckMs"/> before it is believed. A file that really doesn't exist yet
    /// costs that wait once.</para>
    /// </summary>
    internal static FileStream? OpenForFullRead(string path)
    {
        var deadline = Environment.TickCount64 + RenameRetryForMs;
        var missingDeadline = Environment.TickCount64 + (OperatingSystem.IsWindows() ? MissingRecheckMs : 0);
        var backoffMs = 1;
        while (true)
        {
            try
            {
                return new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            }
            catch (Exception ex) when (ex is FileNotFoundException or DirectoryNotFoundException)
            {
                if (Environment.TickCount64 >= missingDeadline)
                {
                    return null;
                }

                Thread.Sleep(1);
            }
            catch (Exception ex) when (IsTransientRenameRefusal(ex) && Environment.TickCount64 < deadline)
            {
                Thread.Sleep(backoffMs);
                backoffMs = Math.Min(backoffMs * 2, 20);
            }
        }
    }

    /// <summary>
    /// Replace <paramref name="path"/> with <paramref name="content"/>: write a temp file beside it, then rename it
    /// over the destination. The rename is atomic, so a reader sees the old file or the new one, never a partial
    /// or missing one.
    ///
    /// <para>The temp name is unique per write. It used to be a fixed <c>path + ".tmp"</c>, which two engines
    /// writing the same sidecar (each sync engine rewrites <c>-info</c>) shared: one renamed it into place, the
    /// other's rename then found its temp file gone and failed with <c>Could not find file '…-info'</c>, failing
    /// that engine's whole operation (TursoAtomicWriteTests).</para>
    ///
    /// <para>On Windows the rename can also be refused outright, <c>Access to the path is denied</c>, while another
    /// rename onto the same file or a read of it is in flight (measured: 135 of 600 renames by four concurrent
    /// writers, with no reader at all; <see cref="FileShare.Delete"/> on the reader did not help). That refusal lasts
    /// only as long as the other operation, so the rename is retried briefly. A missing file is never retried: that
    /// is a real failure, not contention.</para>
    /// </summary>
    internal static void WriteFileAtomically(string path, byte[] content)
    {
        var dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(dir))
        {
            Directory.CreateDirectory(dir);
        }

        // Same directory as the destination, so the rename never crosses a filesystem (where it stops being atomic).
        var tmp = $"{path}.{Guid.NewGuid():N}.tmp";
        try
        {
            File.WriteAllBytes(tmp, content);
            MoveReplacingWithRetry(tmp, path);
        }
        catch
        {
            try { File.Delete(tmp); } catch { /* best effort: never mask the original failure */ }
            throw;
        }
    }

    private const int RenameRetryForMs = 2000;
    private const int MissingRecheckMs = 50;

    private static void MoveReplacingWithRetry(string source, string destination)
    {
        var deadline = Environment.TickCount64 + RenameRetryForMs;
        var backoffMs = 1;
        while (true)
        {
            try
            {
                File.Move(source, destination, overwrite: true);
                return;
            }
            catch (Exception ex) when (IsTransientRenameRefusal(ex) && Environment.TickCount64 < deadline)
            {
                Thread.Sleep(backoffMs);
                backoffMs = Math.Min(backoffMs * 2, 20);
            }
        }
    }

    private static bool IsTransientRenameRefusal(Exception ex) =>
        ex is UnauthorizedAccessException
        || ex is IOException and not FileNotFoundException and not DirectoryNotFoundException;

    private static void PumpStream(IntPtr item, Stream stream)
    {
        var buffer = new byte[64 * 1024];
        var handle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
        try
        {
            var ptr = handle.AddrOfPinnedObject();
            int read;
            while ((read = stream.Read(buffer, 0, buffer.Length)) > 0)
            {
                var slice = new TursoSlice { Ptr = ptr, Len = (nuint)read };
                TursoNative.SyncIoPushBuffer(item, ref slice);
            }
        }
        finally
        {
            handle.Free();
        }
    }

    private static void Poison(IntPtr item, string message)
    {
        var bytes = Encoding.UTF8.GetBytes(message);
        if (bytes.Length == 0)
        {
            var empty = default(TursoSlice);
            TursoNative.SyncIoPoison(item, ref empty);
            return;
        }

        var handle = GCHandle.Alloc(bytes, GCHandleType.Pinned);
        try
        {
            var slice = new TursoSlice { Ptr = handle.AddrOfPinnedObject(), Len = (nuint)bytes.Length };
            TursoNative.SyncIoPoison(item, ref slice);
        }
        finally
        {
            handle.Free();
        }
    }

    // ---- helpers ---------------------------------------------------------------------------------

    internal static void Check(TursoStatus status, IntPtr errorPtr, string ctx)
    {
        if (errorPtr != IntPtr.Zero)
        {
            var message = Marshal.PtrToStringUTF8(errorPtr);
            TursoNative.FreeString(errorPtr);
            // Carry the status even when the engine attached a message — otherwise lock contention is
            // indistinguishable from a genuine error and callers cannot know what is safe to retry.
            throw new TursoException($"{ctx}: {message}", status);
        }

        if (status is TursoStatus.Ok or TursoStatus.Done or TursoStatus.Row)
        {
            return;
        }

        throw new TursoException($"{ctx}: status {status}", status);
    }

    /// <summary>
    /// Marshal an HTTP request-body slice, preserving the None/Some(empty) distinction the sync engine relies
    /// on. A null pointer means the engine attached no body; a non-null pointer with length 0 means it attached
    /// an empty body (an all-default protobuf — e.g. the initial <c>/pull-updates</c> request — encodes to zero
    /// bytes, which the sdk-kit hands back as a dangling-but-non-null <c>0x1</c> slice). The empty case must
    /// still produce a body: without one the <c>content-type</c> header cannot attach and no
    /// <c>Content-Length: 0</c> is sent, and Turso Cloud then content-negotiates <c>/pull-updates</c> as JSON
    /// and rejects it with HTTP 400. The self-hosted sync server tolerates a missing body because it decodes as
    /// protobuf, which is why this only surfaced against the cloud round-trip.
    /// </summary>
    internal static byte[]? BodyFromSlice(TursoSlice slice) =>
        slice.Ptr == IntPtr.Zero ? null : SliceToBytes(slice) ?? [];

    /// <summary>
    /// Build the outgoing <see cref="HttpRequestMessage"/> from the sync engine's request: attach the body
    /// (when present, including an empty one), route each engine header to request- or content-headers, add the
    /// bearer auth and a default User-Agent, and set the Host. Pure and native-free so it can be unit-tested.
    /// </summary>
    internal static HttpRequestMessage BuildHttpMessage(
        string method,
        string url,
        byte[]? body,
        IReadOnlyList<KeyValuePair<string, string>> headers,
        string? authToken,
        string? host)
    {
        var message = new HttpRequestMessage(new HttpMethod(method), url);
        if (body is not null)
        {
            message.Content = new ByteArrayContent(body);
        }

        foreach (var header in headers)
        {
            if (header.Key.Length == 0)
            {
                continue;
            }

            if (!message.Headers.TryAddWithoutValidation(header.Key, header.Value))
            {
                message.Content?.Headers.TryAddWithoutValidation(header.Key, header.Value);
            }
        }

        if (!string.IsNullOrEmpty(authToken))
        {
            message.Headers.TryAddWithoutValidation("Authorization", "Bearer " + authToken);
        }

        if (!message.Headers.Contains("User-Agent"))
        {
            message.Headers.TryAddWithoutValidation("User-Agent", "tursosync");
        }

        message.Headers.Host = host;
        return message;
    }

    private static byte[]? SliceToBytes(TursoSlice slice)
    {
        if (slice.Ptr == IntPtr.Zero || slice.Len == 0)
        {
            return null;
        }

        var n = checked((int)slice.Len);
        var dst = new byte[n];
        Marshal.Copy(slice.Ptr, dst, 0, n);
        return dst;
    }

    private static string SliceToString(TursoSlice slice)
    {
        var bytes = SliceToBytes(slice);
        return bytes is null ? string.Empty : Encoding.UTF8.GetString(bytes);
    }

    internal static string NormalizeUrl(string url) =>
        url.StartsWith("libsql://", StringComparison.OrdinalIgnoreCase)
            ? "https://" + url["libsql://".Length..]
            : url;

    internal static string JoinUrl(string baseUrl, string path)
    {
        if (!path.StartsWith('/'))
        {
            path = "/" + path;
        }

        return baseUrl.TrimEnd('/') + path;
    }

    internal static string? BuildHost(string baseUrl, string? ns)
    {
        if (string.IsNullOrEmpty(baseUrl) || !Uri.TryCreate(baseUrl, UriKind.Absolute, out var uri))
        {
            return null;
        }

        return string.IsNullOrEmpty(ns) ? uri.Host : ns + "." + uri.Host;
    }

    private static string? Trim(string? value) => string.IsNullOrWhiteSpace(value) ? null : value.Trim();

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, this);

    /// <summary>Pins and owns UTF-8 C strings for the duration of a single native call.</summary>
    private sealed class MarshaledStrings : IDisposable
    {
        private readonly List<IntPtr> _ptrs = [];

        public IntPtr Utf8(string value)
        {
            var ptr = Marshal.StringToCoTaskMemUTF8(value);
            _ptrs.Add(ptr);
            return ptr;
        }

        public IntPtr Utf8OrNull(string? value) => string.IsNullOrEmpty(value) ? IntPtr.Zero : Utf8(value);

        public void Dispose()
        {
            foreach (var ptr in _ptrs)
            {
                Marshal.FreeCoTaskMem(ptr);
            }

            _ptrs.Clear();
        }
    }
}
