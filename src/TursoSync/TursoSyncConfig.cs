namespace Turso.Sync;

/// <summary>Supported ciphers for local at-rest database encryption (mirrors the native engine).</summary>
public enum TursoEncryptionCipher
{
    /// <summary>AES-128-GCM.</summary>
    Aes128Gcm,

    /// <summary>AES-256-GCM.</summary>
    Aes256Gcm,

    /// <summary>AEGIS-256.</summary>
    Aegis256,

    /// <summary>AEGIS-256X2.</summary>
    Aegis256x2,

    /// <summary>AEGIS-128L.</summary>
    Aegis128l,

    /// <summary>AEGIS-128X2.</summary>
    Aegis128x2,

    /// <summary>AEGIS-128X4.</summary>
    Aegis128x4,
}

/// <summary>Cipher name helpers.</summary>
public static class TursoEncryptionCipherExtensions
{
    /// <summary>The native cipher name (e.g. <c>aes256gcm</c>).</summary>
    public static string ToName(this TursoEncryptionCipher cipher) => cipher switch
    {
        TursoEncryptionCipher.Aes128Gcm => "aes128gcm",
        TursoEncryptionCipher.Aes256Gcm => "aes256gcm",
        TursoEncryptionCipher.Aegis256 => "aegis256",
        TursoEncryptionCipher.Aegis256x2 => "aegis256x2",
        TursoEncryptionCipher.Aegis128l => "aegis128l",
        TursoEncryptionCipher.Aegis128x2 => "aegis128x2",
        TursoEncryptionCipher.Aegis128x4 => "aegis128x4",
        _ => throw new ArgumentOutOfRangeException(nameof(cipher), cipher, null),
    };
}

/// <summary>
/// Configuration for a synced Turso database. With <see cref="RemoteUrl"/> unset the database is purely
/// local (offline): the engine only issues file IO and never HTTP, which is the free/local tier. Set
/// <see cref="RemoteUrl"/> + <see cref="AuthToken"/> to sync against Turso Cloud.
/// </summary>
public sealed record TursoSyncConfig
{
    /// <summary>Path to the main local database file. Auxiliary files derive their names from it.</summary>
    public required string Path { get; init; }

    /// <summary>Remote sync URL (<c>libsql://…</c>, <c>https://…</c>); null/empty for local-only.</summary>
    public string? RemoteUrl { get; init; }

    /// <summary>Remote namespace prefix (sent as the request <c>Host</c>), optional.</summary>
    public string? Namespace { get; init; }

    /// <summary>Bearer token for remote auth; sent as <c>Authorization: Bearer …</c>.</summary>
    public string? AuthToken { get; init; }

    /// <summary>Client-id prefix the engine records in metadata; defaults to <c>tursosync</c>.</summary>
    public string ClientName { get; init; } = "tursosync";

    /// <summary>Long-poll timeout (ms) the server holds a pull open waiting for changes; 0 = off.</summary>
    public int LongPollTimeoutMs { get; init; }

    /// <summary>The default number of CDC operations per push batch.</summary>
    public const int DefaultPushOperationsThreshold = 1000;

    /// <summary>
    /// Split pushes into HTTP batches once this many CDC operations have accumulated. Batches end only at
    /// transaction boundaries, so a single larger transaction is never split. Zero sends the whole change set
    /// in one request. Bounded batches let an offline backlog make progress without one enormous upload.
    /// <para>ADO.NET connections sharing a replica must use the same threshold. Close them and call
    /// <see cref="TursoConnection.ClearPool"/> before reopening with a different threshold.</para>
    /// </summary>
    public int PushOperationsThreshold { get; init; } = DefaultPushOperationsThreshold;

    /// <summary>The <see cref="HttpTimeoutMs"/> used when it is 0: one minute.</summary>
    public const int DefaultHttpTimeoutMs = 60_000;

    /// <summary>
    /// How long (ms) a sync request may wait on the server: for the response to start, and then for each further
    /// read of its body. 0 (default) uses <see cref="DefaultHttpTimeoutMs"/>; &lt;0 waits forever. A request that runs
    /// out fails its push or pull with a <see cref="TursoException"/> naming the timeout.
    /// <para>A push or pull holds the sync database for its whole round trip, and a new connection can't be opened
    /// until it ends, so this also bounds how long a dead network can stall opening one.</para>
    /// <para>It never cuts a long poll short: with <see cref="LongPollTimeoutMs"/> set, a request waits at least
    /// that long plus <see cref="DefaultHttpTimeoutMs"/>.</para>
    /// <para>The engine is shared by every connection to a replica, so the first one to open it sets this.</para>
    /// </summary>
    public int HttpTimeoutMs { get; init; }

    /// <summary>
    /// Optional credential-safe HTTP completion callback, including requests during create/bootstrap.
    /// Null (off) by default. Initializes <see cref="TursoSyncDatabase.HttpDiagnostics"/>; see that property
    /// for callback threading and shared-replica semantics. Callback exceptions are ignored.
    /// </summary>
    public Action<TursoSyncHttpDiagnostic>? HttpDiagnostics { get; init; }

    /// <summary>The timeout <see cref="HttpTimeoutMs"/> and <see cref="LongPollTimeoutMs"/> come to.</summary>
    internal TimeSpan EffectiveHttpTimeout
    {
        get
        {
            if (HttpTimeoutMs < 0)
            {
                return Timeout.InfiniteTimeSpan;
            }

            var ms = HttpTimeoutMs == 0 ? DefaultHttpTimeoutMs : HttpTimeoutMs;
            if (LongPollTimeoutMs > 0)
            {
                ms = Math.Max(ms, LongPollTimeoutMs + DefaultHttpTimeoutMs);
            }

            return TimeSpan.FromMilliseconds(ms);
        }
    }

    /// <summary>
    /// Bootstrap an empty DB from the remote on create. Must be false for local-only use (true requires
    /// the network to be online to connect to a fresh DB).
    /// </summary>
    public bool BootstrapIfEmpty { get; init; }

    /// <summary>Busy timeout (ms) for the connection; 0 uses the engine default, &lt;0 disables it.</summary>
    public int BusyTimeoutMs { get; init; } = 5000;

    /// <summary>
    /// Optional hint, in bytes, to chunk the initial bootstrap download into multiple <c>/pull-updates</c>
    /// requests (each covering the smallest contiguous page range whose size reaches this threshold), rather
    /// than fetching the whole database in one round-trip. 0 (default) bootstraps in a single request. Useful
    /// for large remote databases where a single response would be unwieldy.
    /// </summary>
    public long PullBytesThreshold { get; init; }

    /// <summary>
    /// Local at-rest encryption cipher name (e.g. <c>aes256gcm</c>); null disables encryption.
    /// <para><b>Base-engine lane only.</b> At-rest encryption applies to a local-only database (no
    /// <see cref="RemoteUrl"/>, sync disabled). The sync engine does not support it — creating a synced
    /// database with a cipher set throws <see cref="NotSupportedException"/>, because the engine cannot
    /// reopen the encrypted local file. (Turso Cloud server-side encryption is a separate, remote concept.)</para>
    /// </summary>
    public string? EncryptionCipher { get; init; }

    /// <summary>Hex-encoded encryption key. Required when <see cref="EncryptionCipher"/> is set. Base-lane only — see <see cref="EncryptionCipher"/>.</summary>
    public string? EncryptionKey { get; init; }

    /// <summary>
    /// Enable the engine's experimental <c>index_method</c> feature — required for tantivy full-text search
    /// (<c>CREATE INDEX … USING fts</c>, <c>fts_match</c>/<c>fts_score</c>/<c>fts_highlight</c>,
    /// <c>OPTIMIZE INDEX</c>). Off by default; without it those statements fail to parse. Composes with
    /// <see cref="EncryptionCipher"/> — both features are sent to the native together — so an FTS index
    /// built inside an encrypted database is itself encrypted at rest.
    /// <para><b>Experimental:</b> the feature is behind an engine flag and its behaviour/API may change with
    /// engine bumps.</para>
    /// </summary>
    public bool ExperimentalIndexMethod { get; init; }

    /// <summary>
    /// Schema protection around the sync operations that can rewrite local state (create/open of an
    /// existing replica, applying pulled changes). Default <see cref="TursoSchemaGuardMode.Detect"/>:
    /// a sync operation that drops local user tables throws <see cref="TursoSchemaGuardException"/>
    /// instead of losing them silently. Only active when a remote is configured — a local-only database
    /// never reconciles against a server. See <see cref="TursoSchemaGuardMode"/>.
    /// </summary>
    public TursoSchemaGuardMode SchemaGuard { get; init; } = TursoSchemaGuardMode.Detect;

    /// <summary>True when local at-rest encryption is configured.</summary>
    public bool IsEncrypted => !string.IsNullOrWhiteSpace(EncryptionCipher);
}
