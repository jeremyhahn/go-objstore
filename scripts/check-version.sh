#!/usr/bin/env bash
#
# check-version.sh — Verify every SDK manifest matches the root VERSION file.
#
# Exits 0 if all SDK version declarations equal the root VERSION value.
# Exits 1 and lists each mismatch otherwise. This is the verification
# counterpart to scripts/sync-version.sh.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

VERSION_FILE="$ROOT_DIR/VERSION"
if [[ ! -f "$VERSION_FILE" ]]; then
	echo "error: VERSION file not found at $VERSION_FILE" >&2
	exit 1
fi

VERSION="$(tr -d '[:space:]' < "$VERSION_FILE")"
SDKS="$ROOT_DIR/api/sdks"
MISMATCH=0

# extract prints capture group 1 of the first line in <file> matching the
# full-line extended-regex <regex> (which must capture the version in group
# 1 and match the entire line), or nothing if the file/pattern is absent.
# Uses a control-char delimiter so regexes may contain literal slashes
# (e.g. the C# </Version> closing tag).
extract() {
	local file="$1" regex="$2"
	[[ -f "$file" ]] || { echo ""; return 0; }
	sed -nE $'s\001'"${regex}"$'\001\\1\001p' "$file" | head -n1
}

# verify compares an extracted value against the expected version.
verify() {
	local label="$1" got="$2"
	if [[ "$got" == "$VERSION" ]]; then
		echo "  ok       $label = $got"
	elif [[ -z "$got" ]]; then
		echo "  MISSING  $label (could not read version)"
		MISMATCH=1
	else
		echo "  MISMATCH $label = $got (expected $VERSION)"
		MISMATCH=1
	fi
}

echo "Checking SDK versions against root VERSION = $VERSION"

verify "go (version.go)" \
	"$(extract "$SDKS/go/version.go" "^const Version = \"([^\"]*)\".*$")"

verify "python (setup.py)" \
	"$(extract "$SDKS/python/setup.py" "^[[:space:]]*version=\"([^\"]*)\".*$")"

verify "python (pyproject.toml)" \
	"$(extract "$SDKS/python/pyproject.toml" "^version[[:space:]]*=[[:space:]]*\"([^\"]*)\".*$")"

verify "python (__init__.py)" \
	"$(extract "$SDKS/python/objstore/__init__.py" "^__version__[[:space:]]*=[[:space:]]*\"([^\"]*)\".*$")"

verify "ruby (version.rb)" \
	"$(extract "$SDKS/ruby/lib/objstore/version.rb" "^[[:space:]]*VERSION[[:space:]]*=[[:space:]]*\"([^\"]*)\".*$")"

verify "ruby (gemspec)" \
	"$(extract "$SDKS/ruby/objstore.gemspec" "^[[:space:]]*spec\.version[[:space:]]*=[[:space:]]*\"([^\"]*)\".*$")"

verify "rust (Cargo.toml)" \
	"$(extract "$SDKS/rust/Cargo.toml" "^version[[:space:]]*=[[:space:]]*\"([^\"]*)\".*$")"

verify "typescript (package.json)" \
	"$(extract "$SDKS/typescript/package.json" "^[[:space:]]*\"version\"[[:space:]]*:[[:space:]]*\"([^\"]*)\".*$")"

verify "csharp (ObjStore.SDK.csproj)" \
	"$(extract "$SDKS/csharp/ObjStore.SDK.csproj" "^.*<Version>([^<]*)</Version>.*$")"

echo ""
if [[ "$MISMATCH" -eq 0 ]]; then
	echo "All SDK versions match $VERSION."
	exit 0
else
	echo "One or more SDK versions do not match the root VERSION ($VERSION)."
	exit 1
fi
