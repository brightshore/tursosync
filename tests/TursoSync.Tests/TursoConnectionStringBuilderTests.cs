using Turso.Sync;

namespace TursoSync.Tests;

/// <summary>
/// Pure (no-native) coverage of <see cref="TursoConnectionStringBuilder"/>: typed property accessors,
/// keyword normalization/aliasing, the null-removes-key indexer, and validation. Complements the
/// ToConfig-focused cases in <see cref="TursoSyncBehaviorTests"/>.
/// </summary>
[TestClass]
public class TursoConnectionStringBuilderTests
{
    [TestMethod]
    public void TypedProperties_RoundTrip()
    {
        var b = new TursoConnectionStringBuilder
        {
            DataSource = "x.db",
            RemoteUrl = "libsql://h",
            AuthToken = "tok",
            Namespace = "ns",
            Bootstrap = true,
            DefaultTimeout = 15,
            BusyTimeout = 2500,
            LongPollTimeout = 1000,
            Sync = true,
        };

        b.DataSource.Should().Be("x.db");
        b.RemoteUrl.Should().Be("libsql://h");
        b.AuthToken.Should().Be("tok");
        b.Namespace.Should().Be("ns");
        b.Bootstrap.Should().BeTrue();
        b.DefaultTimeout.Should().Be(15);
        b.BusyTimeout.Should().Be(2500);
        b.LongPollTimeout.Should().Be(1000);
        b.Sync.Should().BeTrue();
    }

    [TestMethod]
    public void Defaults_AreApplied()
    {
        var b = new TursoConnectionStringBuilder();
        b.DefaultTimeout.Should().Be(30);
        b.BusyTimeout.Should().Be(5000);
        b.LongPollTimeout.Should().Be(0);
        b.Pooling.Should().BeTrue();        // default true when key absent
        b.Sync.Should().BeFalse();
        b.DataSource.Should().BeEmpty();
    }

    [TestMethod]
    public void Pooling_ExplicitFalse_Honored()
    {
        new TursoConnectionStringBuilder("Data Source=x.db;Pooling=false").Pooling.Should().BeFalse();
    }

    [TestMethod]
    public void DefaultTimeout_Negative_Throws()
    {
        var b = new TursoConnectionStringBuilder();
        var act = () => b.DefaultTimeout = -1;
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [TestMethod]
    public void PushOperationsThreshold_Negative_Setter_Throws()
    {
        var b = new TursoConnectionStringBuilder();
        var act = () => b.PushOperationsThreshold = -1;
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [TestMethod]
    [DataRow("Push Operations Threshold")]
    [DataRow("PushOperationsThreshold")]
    public void PushOperationsThreshold_Negative_Parsed_Or_Indexed_Value_Throws(string keyword)
    {
        var parsed = new TursoConnectionStringBuilder($"Data Source=x.db;{keyword}=-5");
        var indexed = new TursoConnectionStringBuilder("Data Source=x.db") { [keyword] = -5 };
        foreach (var b in new[] { parsed, indexed })
        {
            Action read = () => _ = b.PushOperationsThreshold;
            Action config = () => b.ToConfig();
            Action poolKey = () => _ = b.PoolKey;
            foreach (var act in new[] { read, config, poolKey })
                act.Should().ThrowExactly<ArgumentOutOfRangeException>().Which.ParamName.Should().Be("Push Operations Threshold");
        }
    }

    [TestMethod]
    public void SetEncryption_SetsCipherAndKey()
    {
        var b = new TursoConnectionStringBuilder();
        b.SetEncryption(TursoEncryptionCipher.Aes256Gcm, "deadbeef");

        b.EncryptionCipher.Should().Be("aes256gcm");
        b.EncryptionKey.Should().Be("deadbeef");
    }

    [TestMethod]
    public void KeywordAliases_NormalizeToCanonical()
    {
        var b = new TursoConnectionStringBuilder("DataSource=a.db;RemoteUrl=libsql://h;CommandTimeout=20");
        b.DataSource.Should().Be("a.db");
        b.RemoteUrl.Should().Be("libsql://h");
        b.DefaultTimeout.Should().Be(20);            // CommandTimeout → Default Timeout
        b.ContainsKey("Data Source").Should().BeTrue();
        b.ContainsKey("Filename").Should().BeTrue();  // alias resolves to the same canonical key
    }

    [TestMethod]
    public void Indexer_NullValue_RemovesKey()
    {
        var b = new TursoConnectionStringBuilder("Data Source=a.db;Namespace=ns");
        b.ContainsKey("Namespace").Should().BeTrue();

        b["Namespace"] = null;

        b.ContainsKey("Namespace").Should().BeFalse();
    }

    [TestMethod]
    public void Remove_NormalizesKeyword()
    {
        var b = new TursoConnectionStringBuilder("Data Source=a.db");
        b.Remove("DataSource").Should().BeTrue();    // alias of "Data Source"
        b.ContainsKey("Data Source").Should().BeFalse();
    }

    [TestMethod]
    public void TryGetValue_NormalizesKeyword()
    {
        var b = new TursoConnectionStringBuilder("Data Source=a.db");
        b.TryGetValue("Filename", out var value).Should().BeTrue();
        value.Should().Be("a.db");
    }

    [TestMethod]
    public void UnsupportedKeyword_Throws()
    {
        var b = new TursoConnectionStringBuilder();
        var act = () => b["Bogus"] = "1";
        act.Should().Throw<ArgumentException>();
    }
}
