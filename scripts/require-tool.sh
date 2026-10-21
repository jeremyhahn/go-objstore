#!/usr/bin/env bash
# require-tool.sh -- fail closed when a gate tool is missing or the wrong version.
#
# Usage: require-tool.sh <tool> [<version>]
#
# Looks for <tool> on PATH, then in $GOBIN / $GOPATH/bin. On success prints the
# resolved path on stdout and exits 0. When the tool is absent, or <version> is
# given and the tool's version output does not contain it, prints an install
# hint on stderr and exits 1. A gate that cannot run must fail, never pass
# vacuously (go-objstore#37).
set -u

if [ $# -lt 1 ] || [ $# -gt 2 ]; then
	echo "usage: require-tool.sh <tool> [<version>]" >&2
	exit 2
fi

tool=$1
want=${2:-}

hint() {
	case "$1" in
	trivy) echo "https://trivy.dev/latest/getting-started/installation/" ;;
	actionlint) echo "go install github.com/rhysd/actionlint/cmd/actionlint@latest" ;;
	golangci-lint) echo "curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b \$(go env GOPATH)/bin v${want:-2.14.0}" ;;
	gosec | govulncheck) echo "make install-security-tools" ;;
	*) echo "install '$1' and put it on PATH" ;;
	esac
}

gobin=${GOBIN:-${GOPATH:-${HOME:-}/go}/bin}

bin=""
if found=$(command -v "$tool" 2>/dev/null) && [ -x "$found" ]; then
	bin=$found
elif [ -x "$gobin/$tool" ]; then
	bin=$gobin/$tool
fi

if [ -z "$bin" ]; then
	echo "require-tool: $tool not found on PATH or in $gobin" >&2
	echo "require-tool: install: $(hint "$tool")" >&2
	exit 1
fi

if [ -n "$want" ]; then
	out=$("$bin" version 2>&1 || "$bin" --version 2>&1)
	case "$out" in
	*"$want"*) ;;
	*)
		echo "require-tool: $tool at $bin is not version $want" >&2
		echo "require-tool: install: $(hint "$tool")" >&2
		exit 1
		;;
	esac
fi

echo "$bin"
