using System.Runtime.InteropServices;
using System.Text.Json;

namespace Turso.Sync;

/// <summary>
/// Validates the on-disk sync state of a synced database BEFORE the native engine opens it, and stamps
/// the engine version that last opened it.
///
/// <para><b>Why.</b> The native sync engine's response to state written by a different engine vintage can
/// be a Rust panic, and a panic crossing the FFI boundary aborts the whole host process — no managed
/// <c>catch</c> ever runs (observed 2026-08-08: <c>turso_sync_operation_resume</c> → <c>abort()</c> during
/// the first pull after an older app rewrote the <c>-info</c> sidecar). Everything here throws a typed,
/// catchable <see cref="TursoSyncStateException"/> instead, while the files are still untouched, so the
/// caller can quarantine + re-bootstrap rather than crash-loop.</para>
///
/// <para>Two checks: the <c>-info</c> metadata must be parseable JSON carrying the metadata version this
/// engine speaks (<c>v1</c>), and the <c>-tursosync</c> stamp must not name a NEWER engine than the one
/// now opening (downgrade refusal — the older engine cannot know what a newer one wrote; serde silently
/// drops fields it doesn't know, which is exactly how foreign state sneaks past the native parser). The
/// newer-opens-older direction is allowed: engines migrate their own state forward.</para>
///
/// <para>Escape hatch: set <c>TURSOSYNC_IGNORE_STATE_GUARD=1</c> to skip validation (the stamp is still
/// written).</para>
/// </summary>
internal static class TursoSyncStateGuard
{
    private const string MetadataVersion = "v1";
    private static string? _engineVersion;

    /// <summary>Suffix of the engine-version stamp sidecar, next to the database file.</summary>
    internal const string StampSuffix = "-tursosync";

    /// <summary>The native engine's version string (e.g. <c>0.7.0</c>), cached after the first call.</summary>
    internal static string EngineVersion =>
        _engineVersion ??= Marshal.PtrToStringUTF8(TursoNative.Version()) ?? "0.0.0";

    private static bool Disabled =>
        Environment.GetEnvironmentVariable("TURSOSYNC_IGNORE_STATE_GUARD") == "1";

    /// <summary>
    /// Validate the sync state for the database at <paramref name="dbPath"/>. No-op when no state exists
    /// yet (a brand-new database) or when the escape hatch is set.
    /// </summary>
    /// <exception cref="TursoSyncStateException">The state is unusable by this engine.</exception>
    internal static void Validate(string dbPath)
    {
        if (Disabled)
        {
            return;
        }

        ValidateMetadata(dbPath + "-info");
        ValidateStamp(dbPath + StampSuffix);
    }

    /// <summary>
    /// Record that THIS engine version now owns the database's sync state. Called after a successful
    /// open/create; best-effort (a failed stamp write must not fail the open that just succeeded).
    /// </summary>
    internal static void Stamp(string dbPath)
    {
        try
        {
            var path = dbPath + StampSuffix;
            var json = JsonSerializer.Serialize(new StampFile
            {
                Engine = EngineVersion,
                StampedUtc = DateTimeOffset.UtcNow.ToString("O"),
            });

            // The same atomic replace as the engine's own sidecar writes, unique temp name included: concurrent
            // creates of one replica each stamp it.
            TursoSyncDatabase.WriteFileAtomically(path, System.Text.Encoding.UTF8.GetBytes(json));
        }
        catch
        {
            // Best effort: the stamp is a guard input, not data.
        }
    }

    private static void ValidateMetadata(string infoPath)
    {
        if (!File.Exists(infoPath))
        {
            return;
        }

        string text;
        try
        {
            text = File.ReadAllText(infoPath);
        }
        catch (Exception ex)
        {
            throw new TursoSyncStateException(
                infoPath, $"sync metadata at '{infoPath}' is unreadable ({ex.GetType().Name}: {ex.Message})");
        }

        // The engine's own FullRead contract treats a missing file as empty — an empty metadata file is
        // therefore equivalent to "no state yet", not corruption.
        if (string.IsNullOrWhiteSpace(text))
        {
            return;
        }

        string? version;
        try
        {
            using var doc = JsonDocument.Parse(text);
            version = doc.RootElement.TryGetProperty("version", out var v) ? v.GetString() : null;
        }
        catch (JsonException ex)
        {
            throw new TursoSyncStateException(
                infoPath,
                $"sync metadata at '{infoPath}' is not parseable JSON ({ex.Message}) — it was likely " +
                "written by an incompatible engine. Quarantine the database's sidecar files and " +
                "re-bootstrap from the remote, or restore a backup.");
        }

        if (version != MetadataVersion)
        {
            throw new TursoSyncStateException(
                infoPath,
                $"sync metadata at '{infoPath}' carries version '{version ?? "(none)"}' but this engine " +
                $"speaks '{MetadataVersion}'. Quarantine the database's sidecar files and re-bootstrap " +
                "from the remote, or open with the engine that wrote it.");
        }
    }

    private static void ValidateStamp(string stampPath)
    {
        if (!File.Exists(stampPath))
        {
            return;
        }

        string? stamped;
        try
        {
            stamped = JsonSerializer.Deserialize<StampFile>(File.ReadAllText(stampPath))?.Engine;
        }
        catch
        {
            return; // an unreadable stamp must not brick the database — it regenerates on the next open
        }

        if (TryParseVersion(stamped, out var stampedVersion)
            && TryParseVersion(EngineVersion, out var current)
            && stampedVersion > current)
        {
            throw new TursoSyncStateException(
                stampPath,
                $"this database's sync state was last written by engine {stamped}, which is newer than " +
                $"the engine now opening it ({EngineVersion}). Opening older-on-newer risks a native " +
                "abort or silent state damage. Use the newer engine, or quarantine the sidecar files " +
                "and re-bootstrap from the remote. (Set TURSOSYNC_IGNORE_STATE_GUARD=1 to override.)");
        }
    }

    /// <summary>Parse a semver-ish engine string (<c>0.7.0</c>, tolerating a <c>-suffix</c>).</summary>
    private static bool TryParseVersion(string? value, out Version version)
    {
        version = new Version(0, 0);
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        var core = value.Split('-', '+')[0];
        return Version.TryParse(core, out version!);
    }

    private sealed class StampFile
    {
        public string? Engine { get; set; }
        public string? StampedUtc { get; set; }
    }
}
