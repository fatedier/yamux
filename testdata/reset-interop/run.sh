#!/bin/sh
# Compare this worktree with a locally available historical revision without
# adding an old-version dependency to the library or changing any git refs.
set -eu
interop_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
interop_revision=${1:-c38d75f}
if [ "$#" -gt 0 ]; then shift; fi
interop_commit=$(git -C "$interop_root" rev-parse --verify "$interop_revision^{commit}")
interop_tmp=$(mktemp -d "${TMPDIR:-/tmp}/yamux-reset-interop.XXXXXX")
trap 'rm -rf "$interop_tmp"' EXIT HUP INT TERM
mkdir "$interop_tmp/legacy" "$interop_tmp/check"
git -C "$interop_root" archive "$interop_commit" > "$interop_tmp/legacy.tar"
tar -xf "$interop_tmp/legacy.tar" -C "$interop_tmp/legacy"
cd "$interop_tmp/legacy"
go mod edit -module=example.com/yamux-legacy
cp "$interop_root/testdata/reset-interop/interop_test.go" "$interop_tmp/check/"
cd "$interop_tmp/check"
cat > go.mod <<'MOD'
module example.com/yamux-reset-interop

go 1.25.0

require (
    github.com/fatedier/yamux v0.0.0
    example.com/yamux-legacy v0.0.0
)
MOD
go mod edit "-replace=github.com/fatedier/yamux=$interop_root" "-replace=example.com/yamux-legacy=$interop_tmp/legacy"
printf 'Testing Reset against legacy revision %s\n' "$interop_commit"
GOWORK=off GOPROXY=off go test -race -count=1 -timeout=30s "$@" ./...
