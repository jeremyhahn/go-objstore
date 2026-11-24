#!/bin/sh
# gosec-scan.sh <tags>: gosec over pure Go with CGO_ENABLED=0, then cmd/objstorelib with cgo (no -exclude-generated) after a canary; no severity floor.
set -eu

prog=${0##*/}
die() {
  echo "$prog: $*" >&2
  exit 1
}

[ $# -eq 1 ] && [ -n "$1" ] || die "usage: $prog <comma-separated build tags>"
tags=$1
gosec=$(command -v gosec) || die "gosec not found on PATH"
tmp=$(mktemp -d)
canary=cmd/zz-gosec-cgo-canary
trap 'rm -rf "$tmp" "$canary"' EXIT

echo "$prog: pure-Go scan (CGO_ENABLED=0)"
CGO_ENABLED=0 "$gosec" -exclude-generated -tags "$tags" \
  -fmt json -out gosec-results.json -stdout -verbose text \
  -exclude-dir=examples -exclude-dir=cmd/objstorelib ./...

echo "$prog: cgo canary (a planted G115 must be reported)"
mkdir -p "$canary"
printf '%s\n' 'package main' '' '// #include <stdlib.h>' 'import "C"' '' \
  'import "os"' '' 'func main() { _ = C.int(len(os.Args)) }' >"$canary/main.go"
if CGO_ENABLED=1 "$gosec" -tags "$tags" -fmt text "./$canary/..." >"$tmp/canary.log" 2>&1 ||
  ! grep -q 'G115' "$tmp/canary.log"; then
  cat "$tmp/canary.log" >&2
  die "gosec missed the planted G115 in a cgo file; the cgo scan cannot see cgo findings"
fi
rm -rf "$canary"

echo "$prog: cgo scan (cmd/objstorelib, CGO_ENABLED=1)"
rc=0
CGO_ENABLED=1 "$gosec" -tags "$tags" \
  -fmt json -out gosec-cgo-results.json -stdout -verbose text \
  ./cmd/objstorelib/... >"$tmp/cgo.log" 2>&1 || rc=$?
cat "$tmp/cgo.log"
if [ "$rc" -ne 0 ]; then
  echo "$prog: package load errors, if any (cgo failures gosec does not print):" >&2
  CGO_ENABLED=1 go list -e -deps -compiled -tags "$tags" \
    -f '{{if .Error}}{{.ImportPath}}: {{.Error}}{{end}}' ./cmd/objstorelib >&2 || true
  exit "$rc"
fi
grep -q 'Checking package: main$' "$tmp/cgo.log" ||
  die "gosec never analysed cmd/objstorelib; the cgo scan covered nothing"
if grep -q 'Error building the SSA representation of the package main' "$tmp/cgo.log"; then
  die "gosec could not build SSA for cmd/objstorelib, so its SSA rules did not run"
fi
echo "$prog: both scans passed"
