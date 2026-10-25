using System.Runtime.InteropServices;

namespace Turso.Sync;

/// <summary>
/// Turso status codes returned by the C ABI (<c>turso_status_code_t</c>). <see cref="Io"/> means the
/// caller must drive an IO step; <see cref="Done"/> means an async operation finished.
/// </summary>
internal enum TursoStatus
{
    Ok = 0,
    Done = 1,
    Row = 2,
    Io = 3,
    Busy = 4,
    Interrupt = 5,
    BusySnapshot = 6,
    Error = 127,
    Misuse = 128,
    Constraint = 129,
    Readonly = 130,
    DatabaseFull = 131,
    NotADatabase = 132,
    Corrupt = 133,
    IoError = 134,
}

/// <summary>Native value kind for a result column (<c>turso_value_type_t</c>).</summary>
internal enum TursoValueKind
{
    Unknown = 0,
    Integer = 1,
    Real = 2,
    Text = 3,
    Blob = 4,
    Null = 5,
}

/// <summary>Sync-engine IO request kind (<c>turso_sync_io_request_type_t</c>).</summary>
internal enum TursoSyncIoKind
{
    None = 0,
    Http = 1,
    FullRead = 2,
    FullWrite = 3,
}

/// <summary>Async-operation result kind (<c>turso_sync_operation_result_type_t</c>).</summary>
internal enum TursoSyncResultKind
{
    None = 0,
    Connection = 1,
    Changes = 2,
    Stats = 3,
}

/// <summary>
/// Borrowed byte slice (<c>turso_slice_ref_t</c>): <c>{ const void* ptr; size_t len; }</c>. Ownership is
/// never transferred across the ABI, so there is nothing to free here.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
internal struct TursoSlice
{
    public IntPtr Ptr;
    public nuint Len;
}

/// <summary>Base database config (<c>turso_database_config_t</c>). String fields are UTF-8 C pointers.</summary>
[StructLayout(LayoutKind.Sequential)]
internal struct TursoDatabaseConfig
{
    public ulong AsyncIo;
    public IntPtr Path;
    public IntPtr ExperimentalFeatures;
    public IntPtr Vfs;
    public IntPtr EncryptionCipher;
    public IntPtr EncryptionHexKey;
}

/// <summary>
/// Synced database config (<c>turso_sync_database_config_t</c>). Field order, sizes and padding mirror the
/// C header exactly, including the two trailing <c>size_t</c> thresholds the Go binding predates.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
internal struct TursoSyncDatabaseConfig
{
    public IntPtr Path;
    public IntPtr RemoteUrl;
    public IntPtr ClientName;
    public int LongPollTimeoutMs;
    [MarshalAs(UnmanagedType.I1)] public bool BootstrapIfEmpty;
    public int ReservedBytes;
    public int PartialBootstrapStrategyPrefix;
    public IntPtr PartialBootstrapStrategyQuery;
    public nuint PartialBootstrapSegmentSize;
    [MarshalAs(UnmanagedType.I1)] public bool PartialBootstrapPrefetch;
    public IntPtr RemoteEncryptionKey;
    public IntPtr RemoteEncryptionCipher;
    public nuint PushOperationsThreshold;
    public nuint PullBytesThreshold;
}

/// <summary>HTTP IO request fields (<c>turso_sync_io_http_request_t</c>).</summary>
[StructLayout(LayoutKind.Sequential)]
internal struct TursoSyncHttpRequest
{
    public TursoSlice Url;
    public TursoSlice Method;
    public TursoSlice Path;
    public TursoSlice Body;
    public int Headers;
}

/// <summary>HTTP header key/value pair (<c>turso_sync_io_http_header_t</c>).</summary>
[StructLayout(LayoutKind.Sequential)]
internal struct TursoSyncHttpHeader
{
    public TursoSlice Key;
    public TursoSlice Value;
}

/// <summary>Atomic file-read request (<c>turso_sync_io_full_read_request_t</c>).</summary>
[StructLayout(LayoutKind.Sequential)]
internal struct TursoSyncFullReadRequest
{
    public TursoSlice Path;
}

/// <summary>Atomic file-write request (<c>turso_sync_io_full_write_request_t</c>).</summary>
[StructLayout(LayoutKind.Sequential)]
internal struct TursoSyncFullWriteRequest
{
    public TursoSlice Path;
    public TursoSlice Content;
}

/// <summary>Sync stats (<c>turso_sync_stats_t</c>).</summary>
[StructLayout(LayoutKind.Sequential)]
internal struct TursoSyncStats
{
    public long CdcOperations;
    public long MainWalSize;
    public long RevertWalSize;
    public long LastPullUnixTime;
    public long LastPushUnixTime;
    public long NetworkSentBytes;
    public long NetworkReceivedBytes;
    public TursoSlice Revision;
}

/// <summary>
/// P/Invoke surface for the Turso sync SDK kit. A single cdylib (<c>turso_sync_sdk_kit</c>) exports both
/// the base <c>turso_*</c> functions and the <c>turso_sync_*</c> functions, so one library name covers
/// everything — verified empirically (77 base + 29 sync exports, tantivy FTS linked in).
/// </summary>
internal static class TursoNative
{
    internal const string Lib = "turso_sync_sdk_kit";

    // ---- base: database / connection / statement -------------------------------------------------

    /// <summary>The engine's own version string (a pointer to a static C string — never freed).</summary>
    [DllImport(Lib, EntryPoint = "turso_version", CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr Version();

    [DllImport(Lib, EntryPoint = "turso_database_new", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus DatabaseNew(ref TursoDatabaseConfig config, out IntPtr database, out IntPtr errorPtr);

    [DllImport(Lib, EntryPoint = "turso_database_open", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus DatabaseOpen(IntPtr database, out IntPtr errorPtr);

    [DllImport(Lib, EntryPoint = "turso_database_connect", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus DatabaseConnect(IntPtr database, out IntPtr connection, out IntPtr errorPtr);

    [DllImport(Lib, EntryPoint = "turso_database_deinit", CallingConvention = CallingConvention.Cdecl)]
    public static extern void DatabaseDeinit(IntPtr database);

    [DllImport(Lib, EntryPoint = "turso_connection_close", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus ConnectionClose(IntPtr connection, out IntPtr errorPtr);

    [DllImport(Lib, EntryPoint = "turso_connection_deinit", CallingConvention = CallingConvention.Cdecl)]
    public static extern void ConnectionDeinit(IntPtr connection);

    [DllImport(Lib, EntryPoint = "turso_connection_set_busy_timeout_ms", CallingConvention = CallingConvention.Cdecl)]
    public static extern void ConnectionSetBusyTimeoutMs(IntPtr connection, long ms);

    /// <summary>True while the connection is in autocommit mode, i.e. holds no open transaction.</summary>
    [DllImport(Lib, EntryPoint = "turso_connection_get_autocommit", CallingConvention = CallingConvention.Cdecl)]
    [return: MarshalAs(UnmanagedType.U1)]
    public static extern bool ConnectionGetAutocommit(IntPtr connection);

    [DllImport(Lib, EntryPoint = "turso_connection_prepare_single", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus ConnectionPrepareSingle(
        IntPtr connection,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string sql,
        out IntPtr statement,
        out IntPtr errorPtr);

    [DllImport(Lib, EntryPoint = "turso_statement_step", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus StatementStep(IntPtr statement, out IntPtr errorPtr);

    [DllImport(Lib, EntryPoint = "turso_statement_run_io", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus StatementRunIo(IntPtr statement, out IntPtr errorPtr);

    [DllImport(Lib, EntryPoint = "turso_statement_reset", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus StatementReset(IntPtr statement, out IntPtr errorPtr);

    [DllImport(Lib, EntryPoint = "turso_statement_finalize", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus StatementFinalize(IntPtr statement, out IntPtr errorPtr);

    [DllImport(Lib, EntryPoint = "turso_statement_deinit", CallingConvention = CallingConvention.Cdecl)]
    public static extern void StatementDeinit(IntPtr statement);

    [DllImport(Lib, EntryPoint = "turso_statement_n_change", CallingConvention = CallingConvention.Cdecl)]
    public static extern long StatementRowsAffected(IntPtr statement);

    [DllImport(Lib, EntryPoint = "turso_statement_column_count", CallingConvention = CallingConvention.Cdecl)]
    public static extern long StatementColumnCount(IntPtr statement);

    [DllImport(Lib, EntryPoint = "turso_statement_column_name", CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr StatementColumnName(IntPtr statement, nuint index);

    [DllImport(Lib, EntryPoint = "turso_statement_column_decltype", CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr StatementColumnDecltype(IntPtr statement, nuint index);

    [DllImport(Lib, EntryPoint = "turso_statement_row_value_kind", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoValueKind StatementRowValueKind(IntPtr statement, nuint index);

    [DllImport(Lib, EntryPoint = "turso_statement_row_value_bytes_count", CallingConvention = CallingConvention.Cdecl)]
    public static extern long StatementRowValueBytesCount(IntPtr statement, nuint index);

    [DllImport(Lib, EntryPoint = "turso_statement_row_value_bytes_ptr", CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr StatementRowValueBytesPtr(IntPtr statement, nuint index);

    [DllImport(Lib, EntryPoint = "turso_statement_row_value_int", CallingConvention = CallingConvention.Cdecl)]
    public static extern long StatementRowValueInt(IntPtr statement, nuint index);

    [DllImport(Lib, EntryPoint = "turso_statement_row_value_double", CallingConvention = CallingConvention.Cdecl)]
    public static extern double StatementRowValueDouble(IntPtr statement, nuint index);

    [DllImport(Lib, EntryPoint = "turso_statement_named_position", CallingConvention = CallingConvention.Cdecl)]
    public static extern long StatementNamedPosition(
        IntPtr statement,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string name);

    [DllImport(Lib, EntryPoint = "turso_statement_parameters_count", CallingConvention = CallingConvention.Cdecl)]
    public static extern long StatementParameterCount(IntPtr statement);

    [DllImport(Lib, EntryPoint = "turso_statement_parameter_name", CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr StatementParameterName(IntPtr statement, long index);

    [DllImport(Lib, EntryPoint = "turso_statement_bind_positional_null", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus StatementBindNull(IntPtr statement, nuint position);

    [DllImport(Lib, EntryPoint = "turso_statement_bind_positional_int", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus StatementBindInt(IntPtr statement, nuint position, long value);

    [DllImport(Lib, EntryPoint = "turso_statement_bind_positional_double", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus StatementBindDouble(IntPtr statement, nuint position, double value);

    [DllImport(Lib, EntryPoint = "turso_statement_bind_positional_text", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus StatementBindText(IntPtr statement, nuint position, IntPtr ptr, nuint len);

    [DllImport(Lib, EntryPoint = "turso_statement_bind_positional_blob", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus StatementBindBlob(IntPtr statement, nuint position, IntPtr ptr, nuint len);

    [DllImport(Lib, EntryPoint = "turso_str_deinit", CallingConvention = CallingConvention.Cdecl)]
    public static extern void FreeString(IntPtr stringPtr);

    // ---- base: extensibility (UDFs / aggregates / collations / load-extension) -------------------

    [DllImport(Lib, EntryPoint = "turso_connection_register_scalar_function", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus RegisterScalarFunction(
        IntPtr connection,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string name,
        int argc,
        [MarshalAs(UnmanagedType.I1)] bool deterministic,
        IntPtr context,
        TursoScalarFunctionCallback callback,
        TursoContextDestructorCallback contextDestructor,
        TursoValueDestructorCallback valueDestructor,
        out IntPtr errorPtr);

    [DllImport(Lib, EntryPoint = "turso_connection_register_aggregate_function", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus RegisterAggregateFunction(
        IntPtr connection,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string name,
        int argc,
        IntPtr context,
        TursoAggregateInitCallback init,
        TursoAggregateStepCallback step,
        TursoAggregateFinalCallback finalize,
        TursoContextDestructorCallback contextDestructor,
        TursoContextDestructorCallback aggregateDestructor,
        TursoValueDestructorCallback valueDestructor,
        out IntPtr errorPtr);

    [DllImport(Lib, EntryPoint = "turso_connection_unregister_function", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus UnregisterFunction(
        IntPtr connection,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string name,
        out IntPtr errorPtr);

    [DllImport(Lib, EntryPoint = "turso_connection_register_collation", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus RegisterCollation(
        IntPtr connection,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string name,
        IntPtr context,
        TursoCollationCallback callback,
        TursoContextDestructorCallback contextDestructor,
        out IntPtr errorPtr);

    [DllImport(Lib, EntryPoint = "turso_connection_unregister_collation", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus UnregisterCollation(
        IntPtr connection,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string name,
        out IntPtr errorPtr);

    [DllImport(Lib, EntryPoint = "turso_connection_enable_load_extension", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus EnableLoadExtension(
        IntPtr connection,
        [MarshalAs(UnmanagedType.I1)] bool enabled,
        out IntPtr errorPtr);

    [DllImport(Lib, EntryPoint = "turso_connection_load_extension", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus LoadExtension(
        IntPtr connection,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string path,
        out IntPtr errorPtr);

    // ---- sync: database lifecycle + async operations ---------------------------------------------

    [DllImport(Lib, EntryPoint = "turso_sync_database_new", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus SyncDatabaseNew(
        ref TursoDatabaseConfig dbConfig,
        ref TursoSyncDatabaseConfig syncConfig,
        out IntPtr database,
        out IntPtr errorPtr);

    [DllImport(Lib, EntryPoint = "turso_sync_database_open", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus SyncDatabaseOpen(IntPtr database, out IntPtr operation, out IntPtr errorPtr);

    [DllImport(Lib, EntryPoint = "turso_sync_database_create", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus SyncDatabaseCreate(IntPtr database, out IntPtr operation, out IntPtr errorPtr);

    [DllImport(Lib, EntryPoint = "turso_sync_database_connect", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus SyncDatabaseConnect(IntPtr database, out IntPtr operation, out IntPtr errorPtr);

    [DllImport(Lib, EntryPoint = "turso_sync_database_stats", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus SyncDatabaseStats(IntPtr database, out IntPtr operation, out IntPtr errorPtr);

    [DllImport(Lib, EntryPoint = "turso_sync_database_checkpoint", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus SyncDatabaseCheckpoint(IntPtr database, out IntPtr operation, out IntPtr errorPtr);

    [DllImport(Lib, EntryPoint = "turso_sync_database_push_changes", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus SyncDatabasePushChanges(IntPtr database, out IntPtr operation, out IntPtr errorPtr);

    [DllImport(Lib, EntryPoint = "turso_sync_database_wait_changes", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus SyncDatabaseWaitChanges(IntPtr database, out IntPtr operation, out IntPtr errorPtr);

    [DllImport(Lib, EntryPoint = "turso_sync_database_apply_changes", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus SyncDatabaseApplyChanges(IntPtr database, IntPtr changes, out IntPtr operation, out IntPtr errorPtr);

    [DllImport(Lib, EntryPoint = "turso_sync_operation_resume", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus SyncOperationResume(IntPtr operation, out IntPtr errorPtr);

    [DllImport(Lib, EntryPoint = "turso_sync_operation_result_kind", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoSyncResultKind SyncOperationResultKind(IntPtr operation);

    [DllImport(Lib, EntryPoint = "turso_sync_operation_result_extract_connection", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus SyncOperationExtractConnection(IntPtr operation, out IntPtr connection);

    [DllImport(Lib, EntryPoint = "turso_sync_operation_result_extract_changes", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus SyncOperationExtractChanges(IntPtr operation, out IntPtr changes);

    [DllImport(Lib, EntryPoint = "turso_sync_operation_result_extract_stats", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus SyncOperationExtractStats(IntPtr operation, out TursoSyncStats stats);

    [DllImport(Lib, EntryPoint = "turso_sync_operation_deinit", CallingConvention = CallingConvention.Cdecl)]
    public static extern void SyncOperationDeinit(IntPtr operation);

    [DllImport(Lib, EntryPoint = "turso_sync_database_deinit", CallingConvention = CallingConvention.Cdecl)]
    public static extern void SyncDatabaseDeinit(IntPtr database);

    [DllImport(Lib, EntryPoint = "turso_sync_changes_deinit", CallingConvention = CallingConvention.Cdecl)]
    public static extern void SyncChangesDeinit(IntPtr changes);

    // ---- sync: host-driven IO queue --------------------------------------------------------------

    [DllImport(Lib, EntryPoint = "turso_sync_database_io_take_item", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus SyncIoTakeItem(IntPtr database, out IntPtr item, out IntPtr errorPtr);

    [DllImport(Lib, EntryPoint = "turso_sync_database_io_step_callbacks", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus SyncIoStepCallbacks(IntPtr database, out IntPtr errorPtr);

    [DllImport(Lib, EntryPoint = "turso_sync_database_io_request_kind", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoSyncIoKind SyncIoRequestKind(IntPtr item);

    [DllImport(Lib, EntryPoint = "turso_sync_database_io_request_http", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus SyncIoRequestHttp(IntPtr item, out TursoSyncHttpRequest request);

    [DllImport(Lib, EntryPoint = "turso_sync_database_io_request_http_header", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus SyncIoRequestHttpHeader(IntPtr item, nuint index, out TursoSyncHttpHeader header);

    [DllImport(Lib, EntryPoint = "turso_sync_database_io_request_full_read", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus SyncIoRequestFullRead(IntPtr item, out TursoSyncFullReadRequest request);

    [DllImport(Lib, EntryPoint = "turso_sync_database_io_request_full_write", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus SyncIoRequestFullWrite(IntPtr item, out TursoSyncFullWriteRequest request);

    [DllImport(Lib, EntryPoint = "turso_sync_database_io_poison", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus SyncIoPoison(IntPtr item, ref TursoSlice error);

    [DllImport(Lib, EntryPoint = "turso_sync_database_io_status", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus SyncIoStatus(IntPtr item, int status);

    [DllImport(Lib, EntryPoint = "turso_sync_database_io_push_buffer", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus SyncIoPushBuffer(IntPtr item, ref TursoSlice buffer);

    [DllImport(Lib, EntryPoint = "turso_sync_database_io_done", CallingConvention = CallingConvention.Cdecl)]
    public static extern TursoStatus SyncIoDone(IntPtr item);

    [DllImport(Lib, EntryPoint = "turso_sync_database_io_item_deinit", CallingConvention = CallingConvention.Cdecl)]
    public static extern void SyncIoItemDeinit(IntPtr item);
}
