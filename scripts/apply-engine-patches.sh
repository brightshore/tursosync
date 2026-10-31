#!/usr/bin/env bash
# Apply the maintained fixes to the pinned engine before building its native library.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
engine="${1:?usage: apply-engine-patches.sh /path/to/turso}"
for patch in "$here"/patches/*.patch; do
  if git -C "$engine" apply --check "$patch" 2>/dev/null; then
    git -C "$engine" apply "$patch"
  elif git -C "$engine" apply --reverse --check "$patch" 2>/dev/null; then
    echo "Already applied: $(basename "$patch")"
  else
    echo "Engine patch does not match this source: $patch" >&2
    exit 1
  fi
done
