#!/usr/bin/env bash
# Tests for require-tool.sh: a missing or wrong-version tool must exit non-zero
# with an install hint; a present tool must pass. Run: scripts/require-tool_test.sh
set -u

here=$(cd "$(dirname "$0")" && pwd)
script=$here/require-tool.sh
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

mkdir -p "$tmp/bin" "$tmp/gopath/bin"
fail=0

# Isolated environment: PATH holds only the system dirs plus our fake bin dir.
run() {
	env -i HOME="$tmp" GOPATH="$tmp/gopath" PATH="$tmp/bin:/usr/bin:/bin" \
		bash "$script" "$@" >"$tmp/out" 2>"$tmp/err"
}

check() { # name, expected rc, actual rc
	if [ "$2" != "$3" ]; then
		echo "FAIL: $1 (want rc=$2, got rc=$3)"
		fail=1
	else
		echo "ok: $1"
	fi
}

fake() { # name, version output
	printf '#!/bin/sh\necho "%s"\n' "$2" >"$tmp/bin/$1"
	chmod +x "$tmp/bin/$1"
}

for t in trivy actionlint golangci-lint; do
	run "$t"
	check "$t absent exits non-zero" 1 $?
	if ! [ -s "$tmp/err" ] || ! grep -qi 'install' "$tmp/err"; then
		echo "FAIL: $t absent printed no install hint"
		fail=1
	fi
	fake "$t" "$t has version 2.14.0"
	run "$t"
	check "$t present passes" 0 $?
done

run golangci-lint 2.14.0
check "golangci-lint matching version passes" 0 $?

fake golangci-lint "golangci-lint has version 2.9.0"
run golangci-lint 2.14.0
check "golangci-lint wrong version fails" 1 $?
if ! grep -q 'install' "$tmp/err"; then
	echo "FAIL: wrong version printed no install hint"
	fail=1
fi

rm -f "$tmp/bin/trivy"
fake_gopath() { printf '#!/bin/sh\necho ok\n' >"$tmp/gopath/bin/trivy"; chmod +x "$tmp/gopath/bin/trivy"; }
fake_gopath
run trivy
check "tool in GOPATH/bin is found" 0 $?

run
check "no arguments is a usage error" 2 $?

exit $fail
