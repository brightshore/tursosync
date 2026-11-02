using System.Data;
using System.Data.Common;
using System.Diagnostics;
using Turso.Sync;

namespace TursoSync.Tests;

/// <summary>
/// When a transaction takes the write lock. A transaction that reads before its first write holds a snapshot,
/// and any writer committing before that first write makes it stale, which no busy timeout can help. So a
/// default transaction takes the lock at BEGIN, as Microsoft.Data.Sqlite's does.
/// </summary>
[TestClass]
public class TursoTransactionLockingTests
{
    private string _dir = string.Empty;

    [TestInitialize]
    public void Setup()
    {
        if (!TursoNativeLibrary.IsAvailable())
        {
            Assert.Inconclusive("turso_sync_sdk_kit native library not found");
        }

        _dir = Path.Combine(Path.GetTempPath(), "tursosync-tx-" + Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(_dir);
    }

    [TestCleanup]
    public void Teardown()
    {
        TursoConnection.ClearPool();
        if (Directory.Exists(_dir))
        {
            Directory.Delete(_dir, recursive: true);
        }
    }

    private TursoConnection Open(int busyTimeoutMs = 100)
    {
        var conn = new TursoConnection($"Data Source={Path.Combine(_dir, "store.db")};Pooling=False;Busy Timeout={busyTimeoutMs}");
        conn.Open();
        return conn;
    }

    private static long Count(TursoConnection conn)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM t";
        return (long)cmd.ExecuteScalar()!;
    }

    [TestMethod]
    public void Default_Transaction_Read_Then_Write_Survives_Another_Writer()
    {
        using var conn = Open();
        using var other = Open();
        conn.ExecuteNonQuery("CREATE TABLE t (id INTEGER PRIMARY KEY)");

        using var tx = conn.BeginTransaction();
        Count(conn).Should().Be(0);

        // Under a plain BEGIN this commits, and the write below fails with a stale snapshot.
        var otherWrite = () => other.ExecuteNonQuery("INSERT INTO t (id) VALUES (2)");
        otherWrite.Should().Throw<TursoException>().Where(ex => ex.IsBusy);

        conn.ExecuteNonQuery("INSERT INTO t (id) VALUES (1)");
        tx.Commit();

        otherWrite();
        Count(conn).Should().Be(2);
    }

    [TestMethod]
    public async Task BeginTransactionAsync_On_A_DbConnection_Takes_The_Write_Lock_Too()
    {
        using var conn = Open();
        using var other = Open();
        conn.ExecuteNonQuery("CREATE TABLE t (id INTEGER PRIMARY KEY)");

        // The only way in for a caller holding a DbConnection, which can't see the deferred overload.
        DbConnection dbConnection = conn;
        await using var tx = await dbConnection.BeginTransactionAsync();
        Count(conn).Should().Be(0);

        var otherWrite = () => other.ExecuteNonQuery("INSERT INTO t (id) VALUES (2)");
        otherWrite.Should().Throw<TursoException>().Where(ex => ex.IsBusy);

        conn.ExecuteNonQuery("INSERT INTO t (id) VALUES (1)");
        await tx.CommitAsync();
    }

    [TestMethod]
    [DataRow(IsolationLevel.Unspecified)]
    [DataRow(IsolationLevel.Serializable)]
    [DataRow(IsolationLevel.ReadCommitted)]
    public void Isolation_Levels_Hold_The_Write_Lock(IsolationLevel level)
    {
        using var conn = Open();
        using var other = Open();
        conn.ExecuteNonQuery("CREATE TABLE t (id INTEGER PRIMARY KEY)");

        using var tx = conn.BeginTransaction(level);
        var otherWrite = () => other.ExecuteNonQuery("INSERT INTO t (id) VALUES (2)");
        otherWrite.Should().Throw<TursoException>().Where(ex => ex.IsBusy);
    }

    [TestMethod]
    public void Deferred_Transaction_Goes_Stale_When_Another_Writer_Commits()
    {
        using var conn = Open();
        using var other = Open();
        conn.ExecuteNonQuery("CREATE TABLE t (id INTEGER PRIMARY KEY)");

        using var tx = conn.BeginTransaction(deferred: true);
        AssertGoesStale(conn, other);
    }

    [TestMethod]
    public void ReadUncommitted_Stays_Deferred()
    {
        using var conn = Open();
        using var other = Open();
        conn.ExecuteNonQuery("CREATE TABLE t (id INTEGER PRIMARY KEY)");

        using var tx = conn.BeginTransaction(IsolationLevel.ReadUncommitted);
        AssertGoesStale(conn, other);
    }

    private static void AssertGoesStale(TursoConnection conn, TursoConnection other)
    {
        Count(conn).Should().Be(0);
        other.ExecuteNonQuery("INSERT INTO t (id) VALUES (2)");

        var write = () => conn.ExecuteNonQuery("INSERT INTO t (id) VALUES (1)");
        write.Should().Throw<TursoException>().Where(ex => ex.IsBusySnapshot);
    }

    [TestMethod]
    public void Begin_Waits_Out_A_Held_Lock()
    {
        using var holder = Open();
        using var waiter = Open(busyTimeoutMs: 5000);
        holder.ExecuteNonQuery("CREATE TABLE t (id INTEGER PRIMARY KEY)");

        var held = holder.BeginTransaction();
        var release = Task.Run(() =>
        {
            Thread.Sleep(300);
            held.Commit();
        });

        var sw = Stopwatch.StartNew();
        using (var tx = waiter.BeginTransaction())
        {
            sw.Stop();
            tx.Commit();
        }

        release.Wait();
        held.Dispose();
        sw.ElapsedMilliseconds.Should().BeInRange(200, 4000, "BEGIN waits for the holder, and only for the holder");
    }

    [TestMethod]
    public void Begin_Gives_Up_At_The_Busy_Timeout_And_Leaves_No_Transaction()
    {
        using var holder = Open();
        using var waiter = Open(busyTimeoutMs: 200);
        holder.ExecuteNonQuery("CREATE TABLE t (id INTEGER PRIMARY KEY)");

        using (var held = holder.BeginTransaction())
        {
            var sw = Stopwatch.StartNew();
            var begin = () => waiter.BeginTransaction(IsolationLevel.ReadUncommitted, deferred: false);
            begin.Should().Throw<TursoException>().Where(ex => ex.IsBusy);
            sw.ElapsedMilliseconds.Should().BeGreaterThanOrEqualTo(180, "the wait is what the busy timeout is for");
            waiter.ReadUncommitted.Should().BeFalse("a BEGIN that failed began nothing");
        }

        using var tx = waiter.BeginTransaction();
        tx.Commit();
    }

    [TestMethod]
    [DataRow(true)]
    [DataRow(false)]
    public void Closing_Without_Finishing_A_Transaction_Releases_The_Write_Lock(bool pooling)
    {
        var cs = $"Data Source={Path.Combine(_dir, "store.db")};Pooling={pooling};Busy Timeout=100";
        using var other = Open();
        other.ExecuteNonQuery("CREATE TABLE t (id INTEGER PRIMARY KEY)");

        using (var conn = new TursoConnection(cs))
        {
            conn.Open();
            var abandoned = conn.BeginTransaction();
            conn.ExecuteNonQuery("INSERT INTO t (id) VALUES (1)");
            conn.Close();
        }

        other.ExecuteNonQuery("INSERT INTO t (id) VALUES (2)");
        Count(other).Should().Be(1, "the abandoned transaction rolled back");

        // A pooled connection comes back with no transaction left on it.
        using var reused = new TursoConnection(cs);
        reused.Open();
        using var tx = reused.BeginTransaction();
        tx.Commit();
    }

    [TestMethod]
    public void A_Checkpoint_Never_Makes_A_Default_Transaction_Stale_Or_Strands_Its_Lock()
    {
        var immediate = Contend(deferred: false);
        immediate.Stale.Should().Be(0, "nothing can commit past a transaction holding the write lock");
        immediate.Stranded.Should().Be(0, "disposing a failed transaction ends it, even during a checkpoint");
        immediate.Committed.Should().BeGreaterThan(0);

        // The same load has to be able to produce the failure, or the zero above proves nothing.
        var deferred = Contend(deferred: true);
        deferred.Stale.Should().BeGreaterThan(0);
    }

    private sealed record Contention(int Committed, int Stale, int Stranded);

    private Contention Contend(bool deferred)
    {
        var cs = $"Data Source={Path.Combine(_dir, $"contend-{deferred}.db")};Sync=True;Bootstrap=false;Pooling=False";
        using var conn = new TursoConnection(cs);
        using var other = new TursoConnection(cs);
        conn.Open();
        other.Open();
        conn.ExecuteNonQuery("CREATE TABLE t (id INTEGER PRIMARY KEY, payload TEXT)");
        var db = conn.SyncDatabase!;

        using var done = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var ct = done.Token;
        var payload = new string('p', 1024);

        var writer = Task.Run(() =>
        {
            for (var id = 1_000_000; !ct.IsCancellationRequested; id++)
            {
                try { other.ExecuteNonQuery($"INSERT INTO t VALUES ({id}, '{payload}')"); }
                catch (TursoException) { /* contention is the point */ }
            }
        });
        var checkpointer = Task.Run(() =>
        {
            while (!ct.IsCancellationRequested)
            {
                try { db.Checkpoint(); }
                catch (TursoException) { /* contention is the point */ }
                Thread.Sleep(50);
            }
        });

        var committed = 0;
        var stale = 0;
        var stranded = 0;
        var worker = Task.Run(() =>
        {
            for (var id = 0; !ct.IsCancellationRequested; id++)
            {
                try
                {
                    using var tx = conn.BeginTransaction(deferred);
                    Count(conn);
                    conn.ExecuteNonQuery($"INSERT INTO t VALUES ({id}, 'x')");
                    tx.Commit();
                    committed++;
                }
                catch (TursoException ex)
                {
                    stale += ex.IsBusySnapshot ? 1 : 0;
                }

                if (conn.Raw.InTransaction)
                {
                    stranded++;
                    conn.Raw.Rollback();
                }
            }
        });

        Task.WaitAll(writer, checkpointer, worker);
        return new Contention(committed, stale, stranded);
    }
}
