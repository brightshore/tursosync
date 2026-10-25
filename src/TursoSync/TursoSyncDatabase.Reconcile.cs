using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace Turso.Sync;

/// <summary>Options for <see cref="TursoSyncDatabase.ReconcileLocalTables"/>.</summary>
public sealed record TursoReconcileOptions
{
    /// <summary>Push after a successful rebuild (default true), so the repair reaches the server at once.</summary>
    public bool PushAfter { get; init; } = true;

    /// <summary>
    /// Directory for the throwaway replica used to read the server's table set; default: a fresh directory
    /// under the system temp path, removed afterwards.
    /// </summary>
    public string? ScratchDirectory { get; init; }
}

/// <summary>What <see cref="TursoSyncDatabase.ReconcileLocalTables"/> did.</summary>
/// <param name="RebuiltTables">Tables whose schema was created on the server and whose rows were re-recorded
/// through the synced connection (now in CDC, replicating on push).</param>
/// <param name="SkippedTables">Tables that matched but were not rebuilt (unsupported shape — e.g. generated
/// columns, or no retrievable DDL); listed so the caller knows the repair is incomplete.</param>
/// <param name="RowsCopied">Total rows re-recorded across the rebuilt tables.</param>
public sealed record TursoReconcileResult(
    IReadOnlyList<string> RebuiltTables,
    IReadOnlyList<string> SkippedTables,
    long RowsCopied);

public sealed partial class TursoSyncDatabase
{
    /// <summary>
    /// Teach the server local tables it never learned. The sync engine only replicates ROW changes recorded
    /// after a replica is bound to its remote — DDL never travels, so a table created before the attach is
    /// stranded local-only, its inserts can never push (<c>BATCH_STEP_ERROR: no such table</c>), and a later
    /// revert/reconcile can silently DROP it locally to match the server (the failure
    /// <see cref="TursoSchemaGuardMode"/> detects). This repairs the strand: each affected table's schema
    /// (table + indexes + triggers) is created ON THE SERVER over the Hrana <c>/v2/pipeline</c> endpoint —
    /// the same HTTP surface sync itself uses — and its rows are then re-recorded through the synced
    /// connection (local drop + re-create emits no CDC; the fresh inserts do), so the next push replays them
    /// into a table the server now has.
    /// </summary>
    /// <remarks>
    /// Network required (the server's table set is read via a throwaway bootstrap replica, and the DDL runs
    /// remotely). Each table is rebuilt in its own local transaction with its rows materialized in memory
    /// for the duration — a failure rolls that table back and rethrows; already-rebuilt tables stay done and
    /// re-running is safe. Tables with generated columns are skipped (reported in
    /// <see cref="TursoReconcileResult.SkippedTables"/>). Tables the server already knows are left alone:
    /// re-recording their rows could collide with rows the server holds.
    /// </remarks>
    public TursoReconcileResult ReconcileLocalTables(TursoReconcileOptions? options = null)
    {
        options ??= new TursoReconcileOptions();
        var config = _config
            ?? throw new InvalidOperationException("ReconcileLocalTables needs the database's create config.");
        if (string.IsNullOrEmpty(config.RemoteUrl))
        {
            throw new InvalidOperationException(
                "ReconcileLocalTables requires a remote — a local-only database has no server to reconcile with.");
        }

        // One SQL request over the pipeline lists the server's tables; bootstrapping a scratch replica would
        // download the whole database to read the same names, so it is kept only for a server that will not answer.
        var serverTables = ServerTablesOverPipeline(_http, _baseUrl, _authToken, _namespace)
            ?? FetchServerTables(config, options.ScratchDirectory);

        using var connection = TursoRawConnection.Open(this);
        var local = TursoSchemaGuard.UserTables(connection);
        var known = new HashSet<string>(serverTables, StringComparer.OrdinalIgnoreCase);
        var targets = local.Where(t => !known.Contains(t)).ToList();

        var rebuilt = new List<string>();
        var skipped = new List<string>();
        long rowsCopied = 0;
        foreach (var table in targets)
        {
            var ddl = SchemaSqlOf(connection, "table", table);
            if (ddl is null || ddl.Contains("GENERATED", StringComparison.OrdinalIgnoreCase))
            {
                skipped.Add(table);
                continue;
            }

            var indexes = SchemaSqlsOn(connection, "index", table);
            var triggers = SchemaSqlsOn(connection, "trigger", table);

            // Server first: the table (+ its schema objects) must exist remotely BEFORE any of its row ops
            // push, or the push batch fails. IF NOT EXISTS makes a re-run harmless.
            ExecuteRemoteSql([ddl, .. indexes, .. triggers], idempotent: true);

            rowsCopied += RerecordRows(connection, table, ddl, indexes, triggers);
            rebuilt.Add(table);
        }

        if (options.PushAfter && rebuilt.Count > 0)
        {
            Push();
        }

        return new TursoReconcileResult(rebuilt, skipped, rowsCopied);
    }

    /// <summary>
    /// Re-record one table's rows so they enter CDC: materialize the rows, drop and re-create the table
    /// locally (DDL emits no CDC — only the fresh inserts do), and insert them back, all in one local
    /// transaction so a failure restores the original table.
    /// </summary>
    private static long RerecordRows(
        TursoRawConnection connection, string table, string ddl,
        IReadOnlyList<string> indexes, IReadOnlyList<string> triggers)
    {
        var target = Quote(table);

        var rows = new List<object?[]>();
        int columns;
        using (var select = connection.Prepare($"SELECT * FROM {target}"))
        {
            columns = -1;
            while (select.Step())
            {
                if (columns < 0)
                {
                    columns = select.ColumnCount;
                }

                var row = new object?[columns];
                for (var i = 0; i < columns; i++)
                {
                    row[i] = select.GetValue(i);
                }

                rows.Add(row);
            }

            if (columns < 0)
            {
                columns = select.ColumnCount;
            }
        }

        connection.Execute("BEGIN IMMEDIATE");
        try
        {
            connection.Execute($"DROP TABLE {target}");
            connection.Execute(ddl);
            foreach (var sql in indexes)
            {
                connection.Execute(sql);
            }

            foreach (var sql in triggers)
            {
                connection.Execute(sql);
            }

            if (rows.Count > 0)
            {
                var placeholders = string.Join(", ", Enumerable.Range(1, columns).Select(i => "?" + i));
                using var insert = connection.Prepare($"INSERT INTO {target} VALUES ({placeholders})");
                foreach (var row in rows)
                {
                    insert.Reset();
                    for (var i = 0; i < columns; i++)
                    {
                        insert.Bind(i + 1, row[i]);
                    }

                    while (insert.Step())
                    {
                        // no result rows from an INSERT; drain for completeness
                    }
                }
            }

            connection.Execute("COMMIT");
            return rows.Count;
        }
        catch
        {
            try { connection.Execute("ROLLBACK"); } catch { /* connection state unknown; surface the original */ }
            throw;
        }
    }

    /// <summary>
    /// Execute SQL statements against the remote over the Hrana <c>/v2/pipeline</c> endpoint — the plain
    /// HTTP SQL surface Turso Cloud and <c>tursodb --sync-server</c> both serve — with this database's base
    /// URL, bearer token and namespace Host. <paramref name="idempotent"/> rewrites leading
    /// <c>CREATE TABLE/INDEX/TRIGGER</c> to their <c>IF NOT EXISTS</c> forms so a re-run is harmless.
    /// </summary>
    private void ExecuteRemoteSql(IReadOnlyList<string> sqls, bool idempotent)
    {
        var statements = idempotent ? sqls.Select(MakeIfNotExists).ToList() : sqls.ToList();
        using var doc = SendPipeline(_http, _baseUrl, _authToken, _namespace, statements);
    }

    /// <summary>
    /// POST <paramref name="sqls"/> to the remote's Hrana <c>/v2/pipeline</c> as one batch and return the parsed
    /// response, throwing <see cref="TursoException"/> on a non-2xx status or any statement that did not succeed.
    /// </summary>
    private static JsonDocument SendPipeline(
        HttpClient http, string baseUrl, string? authToken, string? ns, IReadOnlyList<string> sqls)
    {
        var payload = JsonSerializer.Serialize(new
        {
            requests = sqls.Select(sql => (object)new { type = "execute", stmt = new { sql } }).ToArray(),
        });

        using var content = new StringContent(payload, Encoding.UTF8, "application/json");
        using var message = new HttpRequestMessage(HttpMethod.Post, JoinUrl(baseUrl, "/v2/pipeline"))
        {
            Content = content,
        };
        if (!string.IsNullOrEmpty(authToken))
        {
            message.Headers.TryAddWithoutValidation("Authorization", "Bearer " + authToken);
        }

        message.Headers.Host = BuildHost(baseUrl, ns);

        using var response = http.Send(message);
        var body = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
        if (!response.IsSuccessStatusCode)
        {
            throw new TursoException($"remote pipeline: HTTP {(int)response.StatusCode}: {Truncate(body)}");
        }

        var doc = JsonDocument.Parse(body);
        foreach (var result in doc.RootElement.GetProperty("results").EnumerateArray())
        {
            if (result.GetProperty("type").GetString() != "ok")
            {
                doc.Dispose();
                throw new TursoException($"remote pipeline statement failed: {Truncate(body)}");
            }
        }

        return doc;
    }

    /// <summary>
    /// The server's user tables, read with one <c>SELECT</c> over <c>/v2/pipeline</c>, under the same rule as the
    /// local side (<see cref="TursoSchemaGuard.IsInternal"/> names left out). Null when the server cannot answer
    /// that way (an HTTP error, a failed statement, or a response not in the Hrana shape), so the caller can fall
    /// back to reading a bootstrapped replica.
    /// </summary>
    internal static List<string>? ServerTablesOverPipeline(HttpClient http, string baseUrl, string? authToken, string? ns)
    {
        try
        {
            using var doc = SendPipeline(http, baseUrl, authToken, ns,
                ["SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"]);
            var rows = doc.RootElement.GetProperty("results")[0]
                .GetProperty("response").GetProperty("result").GetProperty("rows");
            var tables = new List<string>();
            foreach (var row in rows.EnumerateArray())
            {
                var cell = row[0];
                if (cell.GetProperty("type").GetString() == "text"
                    && cell.GetProperty("value").GetString() is { } name
                    && !TursoSchemaGuard.IsInternal(name))
                {
                    tables.Add(name);
                }
            }

            return tables;
        }
        catch (Exception ex) when (ex is TursoException or HttpRequestException or JsonException
                                       or KeyNotFoundException or InvalidOperationException or IndexOutOfRangeException
                                       or TaskCanceledException)
        {
            return null;
        }
    }

    /// <summary>Rewrite a leading <c>CREATE TABLE/INDEX/UNIQUE INDEX/TRIGGER</c> to its
    /// <c>IF NOT EXISTS</c> form (no-op when already present, or for any other statement).</summary>
    internal static string MakeIfNotExists(string sql) =>
        Regex.Replace(
            sql,
            @"^(\s*CREATE\s+(?:TABLE|(?:UNIQUE\s+)?INDEX|TRIGGER)\s+)(?!IF\s+NOT\s+EXISTS)",
            "$1IF NOT EXISTS ",
            RegexOptions.IgnoreCase | RegexOptions.Singleline);

    /// <summary>The server's user tables, read from a throwaway replica bootstrapped off the remote. This downloads
    /// the whole database, so it is only the fallback for <see cref="ServerTablesOverPipeline"/>.</summary>
    private static List<string> FetchServerTables(TursoSyncConfig config, string? scratchDirectory)
    {
        var dir = scratchDirectory
            ?? Path.Combine(Path.GetTempPath(), "tursosync-reconcile-" + Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(dir);
        try
        {
            using var scratch = Create(config with
            {
                Path = Path.Combine(dir, "scratch.db"),
                BootstrapIfEmpty = true,
                SchemaGuard = TursoSchemaGuardMode.Off, // a fresh scratch replica has nothing to protect
                ClientName = config.ClientName + "-reconcile",
            });
            using var connection = TursoRawConnection.Open(scratch);
            return TursoSchemaGuard.UserTables(connection);
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort scratch cleanup */ }
        }
    }

    /// <summary>The stored DDL of one named schema object, or null when absent (or auto-created, sql NULL).</summary>
    private static string? SchemaSqlOf(TursoRawConnection connection, string type, string name)
    {
        using var stmt = connection.Prepare(
            "SELECT sql FROM sqlite_master WHERE type = ?1 AND name = ?2");
        stmt.Bind(1, type);
        stmt.Bind(2, name);
        return stmt.Step() ? stmt.GetValue(0) as string : null;
    }

    /// <summary>Stored DDL of all objects of <paramref name="type"/> attached to <paramref name="table"/>
    /// (auto-created ones have NULL sql and are skipped — the table DDL re-creates them itself).</summary>
    private static List<string> SchemaSqlsOn(TursoRawConnection connection, string type, string table)
    {
        var sqls = new List<string>();
        using var stmt = connection.Prepare(
            "SELECT sql FROM sqlite_master WHERE type = ?1 AND tbl_name = ?2 AND sql IS NOT NULL");
        stmt.Bind(1, type);
        stmt.Bind(2, table);
        while (stmt.Step())
        {
            if (stmt.GetValue(0) is string sql)
            {
                sqls.Add(sql);
            }
        }

        return sqls;
    }

    private static string Quote(string identifier) => "\"" + identifier.Replace("\"", "\"\"") + "\"";

    private static string Truncate(string body) => body.Length <= 500 ? body : body[..500] + "…";
}
