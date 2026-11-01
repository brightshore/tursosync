#!/usr/bin/env bash
# Exercise Git's actual checkout conversion: a CRLF patch with empty context lines is corrupt to git apply.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
patch_test_root="$(mktemp -d "${TMPDIR:-/tmp}/tursosync-patches.XXXXXX")"
trap 'rm -rf "$patch_test_root"' EXIT

for sdk_crlf in false true; do
  for engine_crlf in false true; do
    case_dir="$patch_test_root/$sdk_crlf-$engine_crlf"
    sdk="$case_dir/sdk"
    engine="$case_dir/engine"
    mkdir -p "$sdk/scripts" "$sdk/patches" "$engine"
    cp "$here/scripts/apply-engine-patches.sh" "$sdk/scripts/"
    if [ -f "$here/.gitattributes" ]; then cp "$here/.gitattributes" "$sdk/"; fi
    cat > "$sdk/patches/fix.patch" <<'PATCH'
diff --git a/file.rs b/file.rs
--- a/file.rs
+++ b/file.rs
@@ -1,3 +1,4 @@
 first

+new
 last
PATCH
    git -C "$sdk" init -q -b fixture
    git -C "$sdk" config core.autocrlf "$sdk_crlf"
    git -C "$sdk" add patches/fix.patch
    if [ -f "$sdk/.gitattributes" ]; then git -C "$sdk" add .gitattributes; fi
    rm "$sdk/patches/fix.patch"
    git -C "$sdk" checkout-index --force --all
    printf 'first\n\nlast\n' > "$engine/file.rs"
    git -C "$engine" init -q -b fixture
    git -C "$engine" config core.autocrlf "$engine_crlf"
    git -C "$engine" add file.rs
    rm "$engine/file.rs"
    git -C "$engine" checkout-index --force --all

    bash "$sdk/scripts/apply-engine-patches.sh" "$engine"
    printf 'first\n\nnew\nlast\n' > "$case_dir/expected"
    tr -d '\r' < "$engine/file.rs" > "$case_dir/actual"
    cmp "$case_dir/expected" "$case_dir/actual"
    bash "$sdk/scripts/apply-engine-patches.sh" "$engine"
    printf 'unexpected source\n' > "$engine/file.rs"
    if bash "$sdk/scripts/apply-engine-patches.sh" "$engine" > "$case_dir/rejected.log" 2>&1; then
      echo "Mismatched source was accepted (SDK autocrlf=$sdk_crlf, engine autocrlf=$engine_crlf)" >&2
      exit 1
    fi
    echo "Passed SDK autocrlf=$sdk_crlf, engine autocrlf=$engine_crlf (apply, idempotency, mismatch)"
  done
done
