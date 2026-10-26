#!/bin/sh
# install-tools.sh — build or download every tool scripts/tool-pins.sh pins
# into one directory. Run by the devcontainer's tools stage
# (build/docker/devcontainer/Dockerfile), with GOPROXY set by the caller.
#
# Usage: sh scripts/install-tools.sh <out-dir>
#
# The Go tools are built with `go install` at their pinned versions. gosec
# builds in a throwaway module against XTOOLS_VERSION: the x/tools its own
# go.mod names cannot read the export data Go 1.27 writes. The release
# archives of trivy and shellcheck are each trusted only when both the archive
# and the binary inside it match the SHA-256s pinned for this architecture.
set -eu

prog=${0##*/}
die() {
  echo "$prog: $*" >&2
  exit 1
}

[ $# -eq 1 ] || die "usage: $prog <out-dir>"
out=$1
here=${0%/*}
# shellcheck source-path=SCRIPTDIR source=tool-pins.sh
. "$here/tool-pins.sh"

mkdir -p "$out"
out=$(cd "$out" && pwd)
GOBIN=$out
export GOBIN

go install "github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$GOLANGCI_VERSION"
go install "golang.org/x/vuln/cmd/govulncheck@$GOVULNCHECK_VERSION"
go install "github.com/rhysd/actionlint/cmd/actionlint@$ACTIONLINT_VERSION"

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

(
  cd "$work"
  mkdir gosec
  cd gosec
  go mod init gosec-build >/dev/null 2>&1
  go get "github.com/securego/gosec/v2/cmd/gosec@$GOSEC_VERSION" "golang.org/x/tools@$XTOOLS_VERSION"
  go install -ldflags "-X main.Version=$GOSEC_VERSION" github.com/securego/gosec/v2/cmd/gosec
)

case $(uname -m) in
  x86_64 | amd64) arch=AMD64 ;;
  aarch64 | arm64) arch=ARM64 ;;
  *) die "unsupported architecture: $(uname -m)" ;;
esac

# verify FILE SHA256: fail unless FILE's SHA-256 is SHA256.
verify() {
  [ -n "$2" ] || die "no pinned SHA-256 for $1"
  echo "$2  $1" | sha256sum -c - >/dev/null || die "$1: SHA-256 mismatch"
}

# fetch URL ARCHIVE_SHA256 MEMBER BIN_SHA256: download URL, verify it, unpack
# MEMBER from it into $out and verify the binary.
fetch() {
  _archive=$work/${1##*/}
  curl -fsSL --retry 3 -o "$_archive" "$1"
  verify "$_archive" "$2"
  mkdir -p "$work/unpack"
  tar -xf "$_archive" -C "$work/unpack" "$3"
  verify "$work/unpack/$3" "$4"
  install -m 0755 "$work/unpack/$3" "$out/${3##*/}"
}

case $arch in
  AMD64) t=64bit s=x86_64 ;;
  ARM64) t=ARM64 s=aarch64 ;;
esac
if [ "$arch" = AMD64 ]; then
  ta=$TRIVY_SHA256_LINUX_AMD64 tb=$TRIVY_BIN_SHA256_LINUX_AMD64
  sa=$SHELLCHECK_SHA256_LINUX_AMD64 sb=$SHELLCHECK_BIN_SHA256_LINUX_AMD64
else
  ta=$TRIVY_SHA256_LINUX_ARM64 tb=$TRIVY_BIN_SHA256_LINUX_ARM64
  sa=$SHELLCHECK_SHA256_LINUX_ARM64 sb=$SHELLCHECK_BIN_SHA256_LINUX_ARM64
fi
fetch "https://github.com/aquasecurity/trivy/releases/download/$TRIVY_VERSION/trivy_${TRIVY_VERSION#v}_Linux-$t.tar.gz" "$ta" trivy "$tb"
fetch "https://github.com/koalaman/shellcheck/releases/download/$SHELLCHECK_VERSION/shellcheck-$SHELLCHECK_VERSION.linux.$s.tar.xz" "$sa" "shellcheck-$SHELLCHECK_VERSION/shellcheck" "$sb"

echo "$prog: installed into $out:"
ls "$out"
