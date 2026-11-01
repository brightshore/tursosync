using System.Runtime.InteropServices;
using Turso.Sync;

namespace TursoSync.Tests;

[TestClass]
public class TursoNativeLayoutTests
{
    [TestMethod]
    public void SyncConfig_MatchesPinnedNativeHeader()
    {
        // sizeof and offsetof from the pinned 0.8.3 turso_sync.h, on all supported 64-bit targets.
        Marshal.SizeOf<TursoSyncDatabaseConfig>().Should().Be(104);
        Marshal.OffsetOf<TursoSyncDatabaseConfig>("LogicalMvccPull").ToInt32().Should().Be(96);
    }
}
