#!/usr/bin/env bash
# mk/consumer-check.sh — post-tag consumer verification of ONE module at ONE tag
# (release-versioning.md §2). Run by `make consumer-check`, and once per module
# by `make release-consumer-check` after release-alpha pushes.
#
# Inputs (environment, set by the Makefile target — never from recipe text):
#   CC_MODULE   the module path, e.g. github.com/jeremyhahn/go-objstore/backup
#   CC_VERSION  the tag of that module, e.g. v0.9.0-alpha
#
# In a scratch module outside the repository, with GOWORK=off, it resolves the
# module at that version exactly as an outside consumer would, builds every
# package in it, and `go install`s each of its commands at that version — which
# the go command refuses for a module whose go.mod carries replace directives.
# It needs the module proxy or the Gitea server the tag was pushed to.
set -euo pipefail

die() { printf 'ERROR: %s\n' "$1" >&2; exit 1; }

mod="${CC_MODULE:-}"
ver="${CC_VERSION:-}"
[ -n "$mod" ] || die 'name the module: make consumer-check MODULE=<module path> VERSION=vX.Y.Z[-pre]'
printf '%s' "$mod" | grep -Eq '^[A-Za-z0-9][A-Za-z0-9.-]*(/[A-Za-z0-9][A-Za-z0-9._~-]*)+$' \
	|| die "not a module path: $mod"
printf '%s' "$ver" | grep -Eq '^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z.-]+)?$' \
	|| die "VERSION is not a module version of the form vX.Y.Z[-pre]: $ver"
command -v go >/dev/null 2>&1 || die 'go is not on PATH'

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
mkdir "$tmp/consumer" "$tmp/bin"
cd "$tmp/consumer"
export GOWORK=off GOFLAGS=-mod=mod GOBIN="$tmp/bin"

go mod init example.invalid/consumer-check >/dev/null 2>&1 || die 'could not create the scratch consumer module'
go get "$mod@$ver" || die "go get $mod@$ver failed: the tag is not resolvable by a consumer"
go build "$mod/..." || die "$mod@$ver does not build for a consumer"
mains="$(go list -f '{{if eq .Name "main"}}{{.ImportPath}}{{end}}' "$mod/...")" \
	|| die "could not list the packages of $mod@$ver"
for p in $mains; do
	go install "$p@$ver" || die "go install $p@$ver failed"
done
echo "consumer check passed: $mod@$ver ($(printf '%s\n' $mains | grep -c . || true) command(s) installed)"
