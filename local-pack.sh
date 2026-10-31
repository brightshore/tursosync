#!/usr/bin/env bash
#
# local-pack.sh — Tier 0 release test (no CI, no secrets).
#
# Builds (or reuses) this host's turso_sync_sdk_kit native, lays it into the package's
# runtimes/<rid>/native/, packs the three TursoSync packages, and (by default) consumes the packed
# TursoSync from a local feed in a throwaway app with TURSOSYNC_NATIVE_DIR UNSET — proving the package
# resolves its own native exactly as a real NuGet consumer would.
#
# Usage:
#   ./local-pack.sh                       # build native (or reuse), pack, consume-test
#   VERSION=0.1.0-dev ./local-pack.sh     # custom package version
#   ./local-pack.sh --no-test             # pack only, skip the consume test
#   ./local-pack.sh --keep                # leave runtimes/ in place after packing
#
# Native source resolution (first hit wins):
#   1. $TURSOSYNC_NATIVE_DIR/<lib>        — reuse an already-built native
#   2. cargo build in $TURSO_SRC          — defaults to ./reference/turso-main (gitignored) if present
#
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
core="$here/src/TursoSync"
version="${VERSION:-0.1.0-local}"
feed="${FEED:-$here/artifacts/local}"
run_test=1
keep=0
for arg in "$@"; do
  case "$arg" in
    --no-test) run_test=0 ;;
    --keep) keep=1 ;;
    *) echo "unknown arg: $arg" >&2; exit 2 ;;
  esac
done

# ---- detect host RID + native lib name ---------------------------------------------------------
case "$(uname -s)" in
  Darwin) os=osx;   lib=libturso_sync_sdk_kit.dylib ;;
  Linux)  os=linux; lib=libturso_sync_sdk_kit.so ;;
  *) echo "Unsupported OS for this script (use the CI release workflow on Windows)." >&2; exit 1 ;;
esac
case "$(uname -m)" in
  arm64|aarch64) arch=arm64 ;;
  x86_64|amd64)  arch=x64 ;;
  *) echo "Unsupported arch: $(uname -m)" >&2; exit 1 ;;
esac
rid="$os-$arch"
echo "→ host RID: $rid  ($lib)"

# ---- resolve the native ------------------------------------------------------------------------
native=""
if [[ -n "${TURSOSYNC_NATIVE_DIR:-}" && -f "$TURSOSYNC_NATIVE_DIR/$lib" ]]; then
  native="$TURSOSYNC_NATIVE_DIR/$lib"
  echo "→ reusing native: $native"
else
  turso_src="${TURSO_SRC:-$here/reference/turso-main}"
  if [[ ! -d "$turso_src" ]]; then
    echo "No native found. Set TURSOSYNC_NATIVE_DIR to a folder containing $lib, or TURSO_SRC to a Turso engine checkout." >&2
    exit 1
  fi
  "$here/scripts/apply-engine-patches.sh" "$turso_src"
  echo "→ building native (release, FTS) from $turso_src ..."
  (cd "$turso_src" && cargo build -p turso_sync_sdk_kit --features turso_sdk_kit/fts --release)
  native="$turso_src/target/release/$lib"
fi

# ---- lay into runtimes/<rid>/native + pack -----------------------------------------------------
dest="$core/runtimes/$rid/native"
mkdir -p "$dest"
cp "$native" "$dest/$lib"
cleanup() { [[ "$keep" -eq 1 ]] || rm -rf "$core/runtimes"; }
trap cleanup EXIT

rm -rf "$feed"
echo "→ packing TursoSync $version → $feed"
dotnet pack "$here/TursoSync.slnx" -c Release -o "$feed" -p:Version="$version" -v q
echo "→ packages:"
ls -1 "$feed"/*.nupkg

# ---- consume test (no env var → must load native from the package) -----------------------------
if [[ "$run_test" -eq 1 ]]; then
  app="$(mktemp -d)/consume"
  mkdir -p "$app"
  cat > "$app/consume.csproj" <<XML
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net10.0</TargetFramework>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
    <RestoreSources>$feed;https://api.nuget.org/v3/index.json</RestoreSources>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="TursoSync" Version="$version" />
  </ItemGroup>
</Project>
XML
  cat > "$app/Program.cs" <<'CS'
using Turso.Sync;
var path = Path.Combine(Path.GetTempPath(), "tursosync-consume-" + Guid.NewGuid().ToString("n"), "t.db");
Directory.CreateDirectory(Path.GetDirectoryName(path)!);
using var c = new TursoConnection($"Data Source={path}");
c.Open();
using (var cmd = c.CreateCommand()) { cmd.CommandText = "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)"; cmd.ExecuteNonQuery(); }
using (var cmd = c.CreateCommand()) { cmd.CommandText = "INSERT INTO t(id,v) VALUES (1,'ok')"; cmd.ExecuteNonQuery(); }
using (var cmd = c.CreateCommand()) { cmd.CommandText = "SELECT v FROM t WHERE id=1"; Console.WriteLine("RESULT=" + cmd.ExecuteScalar()); }
CS
  echo "→ consume test (TURSOSYNC_NATIVE_DIR unset) ..."
  ( unset TURSOSYNC_NATIVE_DIR; cd "$app" && dotnet run -c Release 2>&1 | grep -E "RESULT=|error" )
  echo "✓ consume test passed — package resolves its own native"
fi
