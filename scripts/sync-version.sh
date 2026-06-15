#!/usr/bin/env bash
#
# sync-version.sh — Propagate the root VERSION file to every SDK manifest.
#
# The repository root VERSION file is the single source of truth for the
# release version. This script writes that exact value into each SDK's
# version declaration (Go, Python, Ruby, Rust, JavaScript, TypeScript, C#)
# using precise, anchored edits so only the version value is touched.
#
# The script is idempotent: running it twice produces no further changes.
#
set -euo pipefail

# Resolve the repository root (parent of the scripts/ directory).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

VERSION_FILE="$ROOT_DIR/VERSION"
if [[ ! -f "$VERSION_FILE" ]]; then
	echo "error: VERSION file not found at $VERSION_FILE" >&2
	exit 1
fi

VERSION="$(tr -d '[:space:]' < "$VERSION_FILE")"
if [[ -z "$VERSION" ]]; then
	echo "error: VERSION file is empty" >&2
	exit 1
fi

SDKS="$ROOT_DIR/api/sdks"
CHANGED=0

# escape_sed_replacement escapes characters that are special in a sed
# replacement string (&, \ and the / delimiter) so arbitrary version
# strings are inserted literally.
escape_sed_replacement() {
	printf '%s' "$1" | sed -e 's/[&\/]/\\&/g'
}

VERSION_ESC="$(escape_sed_replacement "$VERSION")"

# apply applies an in-place sed program to a file only if it changes the
# file, reporting what changed. Usage: apply <label> <file> <sed-program>
apply() {
	local label="$1" file="$2" program="$3"
	if [[ ! -f "$file" ]]; then
		echo "  skip   $label (not found: $file)"
		return 0
	fi
	local before after
	before="$(cat "$file")"
	after="$(printf '%s' "$before" | sed -E "$program")"
	if [[ "$before" != "$after" ]]; then
		printf '%s\n' "$after" > "$file"
		# Preserve absence of trailing newline is not required for these
		# manifests; all of them end with a newline.
		echo "  update $label -> $VERSION"
		CHANGED=1
	else
		echo "  ok     $label"
	fi
}

echo "Syncing SDK versions to $VERSION (source: VERSION)"

# Go SDK: const Version = "..."
apply "go (version.go)" "$SDKS/go/version.go" \
	"s/^(const Version = \")[^\"]*(\")/\1${VERSION_ESC}\2/"

# Python: setup.py  ->  version="..."
apply "python (setup.py)" "$SDKS/python/setup.py" \
	"s/^([[:space:]]*version=\")[^\"]*(\",?)/\1${VERSION_ESC}\2/"

# Python: pyproject.toml [tool.poetry]/[project] -> version = "..."
# Anchor on a line that begins with `version =` (no leading whitespace) so
# dependency version specs (which are indented or inline) are not touched.
apply "python (pyproject.toml)" "$SDKS/python/pyproject.toml" \
	"s/^(version[[:space:]]*=[[:space:]]*\")[^\"]*(\")/\1${VERSION_ESC}\2/"

# Python: objstore/__init__.py -> __version__ = "..."
apply "python (__init__.py)" "$SDKS/python/objstore/__init__.py" \
	"s/^(__version__[[:space:]]*=[[:space:]]*\")[^\"]*(\")/\1${VERSION_ESC}\2/"

# Ruby: lib/objstore/version.rb -> VERSION = "..."
apply "ruby (version.rb)" "$SDKS/ruby/lib/objstore/version.rb" \
	"s/^([[:space:]]*VERSION[[:space:]]*=[[:space:]]*\")[^\"]*(\")/\1${VERSION_ESC}\2/"

# Ruby: objstore.gemspec -> spec.version = "..."
apply "ruby (gemspec)" "$SDKS/ruby/objstore.gemspec" \
	"s/^([[:space:]]*spec\.version[[:space:]]*=[[:space:]]*\")[^\"]*(\")/\1${VERSION_ESC}\2/"

# Rust: Cargo.toml [package] version = "..."
# The [package] version is the only top-level `version =` at column 0;
# dependency versions live inside `{ version = ... }` inline tables or are
# indented, so anchoring at start-of-line is sufficient.
apply "rust (Cargo.toml)" "$SDKS/rust/Cargo.toml" \
	"s/^(version[[:space:]]*=[[:space:]]*\")[^\"]*(\")/\1${VERSION_ESC}\2/"

# JavaScript / TypeScript package.json: edit only the top-level "version"
# key. Prefer node so the JSON value is updated robustly; fall back to an
# anchored sed that replaces only the first "version": "..." entry (the
# top-level package version, which npm emits before "dependencies").
sync_package_json() {
	local label="$1" file="$2"
	if [[ ! -f "$file" ]]; then
		echo "  skip   $label (not found: $file)"
		return 0
	fi
	if command -v node >/dev/null 2>&1; then
		local before after
		before="$(cat "$file")"
		after="$(VERSION="$VERSION" node -e '
			const fs = require("fs");
			const f = process.argv[1];
			const j = JSON.parse(fs.readFileSync(f, "utf8"));
			j.version = process.env.VERSION;
			process.stdout.write(JSON.stringify(j, null, 2) + "\n");
		' "$file")"
		if [[ "$before" != "$after" ]]; then
			# Restore the trailing newline stripped by command substitution.
			printf '%s\n' "$after" > "$file"
			echo "  update $label -> $VERSION"
			CHANGED=1
		else
			echo "  ok     $label"
		fi
	else
		# Fallback: replace only the first top-level "version": "..." entry.
		apply "$label" "$file" \
			"0,/^([[:space:]]*\"version\"[[:space:]]*:[[:space:]]*\")[^\"]*(\",?)/s//\1${VERSION_ESC}\2/"
	fi
}

# sync_package_lock updates only the root package version inside an npm
# package-lock.json (lockfileVersion 2/3): the top-level "version" key and
# packages[""].version. Every other "version" entry belongs to a dependency
# and must be left untouched. node is used for a structurally correct edit;
# the sed fallback rewrites only the first two "version" lines, which in an
# npm lockfile are always the root version (line ~3) and packages[""].version
# (the first entry inside `"": {`), both emitted before any dependency block.
sync_package_lock() {
	local label="$1" file="$2"
	if [[ ! -f "$file" ]]; then
		echo "  skip   $label (not found: $file)"
		return 0
	fi
	local before after
	before="$(cat "$file")"
	if command -v node >/dev/null 2>&1; then
		after="$(VERSION="$VERSION" node -e '
			const fs = require("fs");
			const f = process.argv[1];
			const j = JSON.parse(fs.readFileSync(f, "utf8"));
			if (typeof j.version !== "undefined") j.version = process.env.VERSION;
			if (j.packages && j.packages[""] &&
			    typeof j.packages[""].version !== "undefined") {
				j.packages[""].version = process.env.VERSION;
			}
			process.stdout.write(JSON.stringify(j, null, 2) + "\n");
		' "$file")"
	else
		# Fallback: rewrite only the first two "version": "..." lines (the
		# root version and packages[""].version), leaving dependencies alone.
		after="$(printf '%s' "$before" | awk -v ver="$VERSION" '
			{
				if (n < 2 && $0 ~ /^[[:space:]]*"version":[[:space:]]*"[^"]*",?$/) {
					sub(/"version":[[:space:]]*"[^"]*"/, "\"version\": \"" ver "\"")
					n++
				}
				print
			}')"
	fi
	if [[ "$before" != "$after" ]]; then
		# Restore the trailing newline that command substitution strips, so
		# the lockfile keeps the single terminating newline npm writes.
		printf '%s\n' "$after" > "$file"
		echo "  update $label -> $VERSION"
		CHANGED=1
	else
		echo "  ok     $label"
	fi
}

sync_package_json "typescript (package.json)" "$SDKS/typescript/package.json"
sync_package_lock "typescript (package-lock.json)" "$SDKS/typescript/package-lock.json"

# C#: *.csproj -> <Version>...</Version>
apply "csharp (ObjStore.SDK.csproj)" "$SDKS/csharp/ObjStore.SDK.csproj" \
	"s/(<Version>)[^<]*(<\/Version>)/\1${VERSION_ESC}\2/"

if [[ "$CHANGED" -eq 0 ]]; then
	echo "All SDK versions already match $VERSION (no changes)."
else
	echo "Done. SDK versions synced to $VERSION."
fi
