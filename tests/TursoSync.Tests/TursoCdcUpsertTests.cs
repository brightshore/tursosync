using System.Text.Json;
using Turso.Sync;

namespace TursoSync.Tests;

[TestClass]
public class TursoCdcUpsertTests
{
    [TestMethod]
    [DataRow("full", false)]
    [DataRow("before", false)]
    [DataRow("full", true)]
    [DataRow("before", true)]
    public void Upsert_Captures_The_Row_Before_Replacing_It(string mode, bool changeRowid)
    {
        WithConnection(conn =>
        {
            conn.Execute("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)");
            conn.Execute("INSERT INTO items VALUES (1, 'old')");
            conn.Execute($"PRAGMA capture_data_changes_conn('{mode}')");
            conn.Execute(changeRowid
                ? "INSERT INTO items VALUES (1, 'ignored') ON CONFLICT(id) DO UPDATE SET id = 2, value = 'new'"
                : "INSERT INTO items VALUES (1, 'new') ON CONFLICT(id) DO UPDATE SET value = excluded.value");

            var kind = changeRowid ? -1 : 0;
            var json = (string)conn.QueryScalar($"SELECT bin_record_json_object('[\"id\",\"value\"]', before) FROM turso_cdc WHERE table_name = 'items' AND change_type = {kind}")!;
            using var before = JsonDocument.Parse(json);
            before.RootElement.GetProperty("id").GetInt64().Should().Be(1);
            before.RootElement.GetProperty("value").GetString().Should().Be("old");
            conn.QueryScalar("SELECT value FROM items").Should().Be("new");

            if (mode == "full")
            {
                var afterKind = changeRowid ? 1 : 0;
                var afterJson = (string)conn.QueryScalar($"SELECT bin_record_json_object('[\"id\",\"value\"]', after) FROM turso_cdc WHERE table_name = 'items' AND change_type = {afterKind}")!;
                using var after = JsonDocument.Parse(afterJson);
                after.RootElement.GetProperty("id").GetInt64().Should().Be(changeRowid ? 2 : 1);
                after.RootElement.GetProperty("value").GetString().Should().Be("new");
            }
        });
    }

    [TestMethod]
    public void Upsert_Captures_Changes_From_A_Before_Update_Trigger()
    {
        WithConnection(conn =>
        {
            conn.Execute("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT, other)");
            conn.Execute("CREATE TRIGGER touch BEFORE UPDATE ON items BEGIN UPDATE items SET other = 999 WHERE id = NEW.id; END");
            conn.Execute("INSERT INTO items VALUES (1, 'old', 'original')");
            conn.Execute("PRAGMA capture_data_changes_conn('before')");
            conn.Execute("INSERT INTO items VALUES (1, 'new', 'ignored') ON CONFLICT(id) DO UPDATE SET value = excluded.value");
            var json = (string)conn.QueryScalar("SELECT bin_record_json_object('[\"id\",\"value\",\"other\"]', before) FROM turso_cdc WHERE table_name = 'items' AND change_type = 0 ORDER BY change_id DESC LIMIT 1")!;
            using var before = JsonDocument.Parse(json);
            before.RootElement.GetProperty("value").GetString().Should().Be("old");
            before.RootElement.GetProperty("other").GetInt64().Should().Be(999);
        });
    }

    [TestMethod]
    public void Upsert_With_A_Text_Key_And_A_Shorter_Row_Preserves_Valid_Text()
    {
        WithConnection(conn =>
        {
            var original = new string('x', 5000) + "🦊";
            conn.Execute("CREATE TABLE items (id TEXT PRIMARY KEY, value TEXT)");
            conn.Execute($"INSERT INTO items VALUES ('key', '{original}')");
            conn.Execute("PRAGMA capture_data_changes_conn('full')");
            conn.Execute("INSERT INTO items VALUES ('key', 'new') ON CONFLICT(id) DO UPDATE SET value = excluded.value");
            var json = (string)conn.QueryScalar("SELECT bin_record_json_object('[\"id\",\"value\"]', before) FROM turso_cdc WHERE table_name = 'items' AND change_type = 0")!;
            using var before = JsonDocument.Parse(json);
            before.RootElement.GetProperty("value").GetString().Should().Be(original);
            conn.QueryScalar("SELECT value FROM items").Should().Be("new");
        });
    }

    private static void WithConnection(Action<TursoRawConnection> body)
    {
        if (!TursoNativeLibrary.IsAvailable()) Assert.Inconclusive("turso_sync_sdk_kit native library not found");
        var dir = Directory.CreateTempSubdirectory("tursosync-cdc-upsert-");
        try
        {
            using var connection = new TursoConnection(new TursoConnectionStringBuilder
            {
                DataSource = Path.Combine(dir.FullName, "store.db"), Pooling = false,
            }.ConnectionString);
            connection.Open();
            body(connection.Raw);
        }
        finally
        {
            dir.Delete(recursive: true);
        }
    }
}
