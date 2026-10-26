#!/usr/bin/env bash
# release-path-tests.sh — runs go-objstore's release path (mk/release.mk: the
# lockstep check, CHANGELOG fragment assembly and the vendored release-alpha
# template) for real, against a throwaway bare remote, and asserts what reached
# that remote. It covers the defects issue #32 replaced and the assembly step
# release-versioning.md §5 requires; the template's own harness
# (release-alpha-tests.sh) covers its fence and push.followTags cases.
#
#   1. The version is explicit: no VERSION refuses, and nothing is written.
#   2. Every git write goes through the agent-git.sh wrapper, as go-objstore,
#      with exactly the arguments the release contract names.
#   3. The published tag is ANNOTATED, not lightweight.
#   4. A failed commit stops the release: no tag is created, nothing is pushed.
#   5. changelog.d/ fragments and the Unreleased section are assembled into the
#      version's section, and that commit is in the TAGGED tree.
#   6. An empty Unreleased section directly above the previous version heading
#      assembles to nothing, never to a copy of that heading.
#   7. The lockstep guard runs before assembly, even under make -j2.
#   8. The release commit carries the SDK manifest versions synced to the new
#      version (make version-sync), and version-check passes on it.
#
# Nothing here touches the caller's git config or any real remote: HOME and the
# global config are private to each case, and AGENT_GIT is a stub that logs its
# arguments and runs plain git (signing is agent-git.sh's contract).
#
#   bash mk/testdata/release-path-tests.sh
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WIRING="$HERE/../release.mk"
[ -f "$WIRING" ] || { echo "FATAL: no release wiring at $WIRING — the test would measure nothing"; exit 9; }

ROOT="$(mktemp -d)"
trap 'rm -rf "$ROOT"' EXIT
VER=v0.1.0-alpha
PLAIN="${VER#v}"
# The nested modules a fixture releases in lockstep; most fixtures have none.
LOCKSTEP=
TODAY_RE='[0-9]{4}-[0-9]{2}-[0-9]{2}'

ok()  { printf '  PASS  %s\n' "$1"; }
bad() { printf '  FAIL  %s: %s\n' "$1" "$2"; return 1; }

# fixture <case-dir> — a clone of a bare remote on main that includes
# mk/release.mk exactly as the Makefile does, with AGENT_GIT pointed at a stub
# that appends its arguments to $DIR/wrapper.log. FAIL_ON=<git subcommand>
# makes the stub fail that subcommand. Leaves the caller in the clone.
fixture() {
	DIR="$ROOT/$1"
	mkdir -p "$DIR/home"
	export HOME="$DIR/home" GIT_CONFIG_GLOBAL="$DIR/home/gitconfig" GIT_CONFIG_NOSYSTEM=1
	git config --global user.name fixture
	git config --global user.email fixture@example.invalid
	git config --global init.defaultBranch main
	git config --global tag.gpgSign false
	git config --global commit.gpgSign false

	git init -q --bare "$DIR/remote.git"
	git init -q "$DIR/work"
	cd "$DIR/work" || exit 9
	git remote add origin "$DIR/remote.git"

	cat > "$DIR/agent-git" <<EOF
#!/bin/sh
printf '%s\n' "\$*" >> '$DIR/wrapper.log'
[ -n "\${FAIL_ON:-}" ] && [ "\$2" = "\$FAIL_ON" ] && { echo "stub: \$2 failed" >&2; exit 1; }
shift
exec git "\$@"
EOF
	chmod +x "$DIR/agent-git"
	cat > Makefile <<EOF
AGENT_GIT := $DIR/agent-git
LOCKSTEP_MODULES := $LOCKSTEP
RELEASE_REPLACES :=
RELEASE_CONSUMER_MODULES :=
include $WIRING
# Stubs for RELEASE_SYNC_TARGETS, which the template recipe runs after writing
# VERSION; the real ones, the replaces and the consumer check are exercised by
# the sdk_lockstep case below.
version-sync version-check: ; @:
EOF
	printf '0.0.1-alpha\n' > VERSION
	mkdir changelog.d
	: > changelog.d/.gitkeep
	printf '# Changelog\n\n## [Unreleased]\n\n## [0.0.1-alpha] - 2026-01-01\n\n### Added\n\n- The first release.\n' > CHANGELOG.md
	git add -A
	git commit -q -m initial
	git push -q origin main
	: > "$DIR/wrapper.log"
}

# nested <dir> — a fixture whose root module has two nested modules released in
# lockstep, laid out as backup/ and api/sdks/go are here: sdk/go requires the
# root, tool requires both (the SDK at the placeholder version), and each
# reaches them through an in-repo replace. tool also replaces a third-party
# module, which the release must keep.
nested() {
	LOCKSTEP="sdk/go tool" fixture "$1"
	mkdir -p sdk/go tool
	printf 'module example.invalid/fx\n\ngo 1.22\n' > go.mod
	printf 'module example.invalid/fx/sdk/go\n\ngo 1.22\n\nrequire example.invalid/fx v0.0.1-alpha\n\nreplace example.invalid/fx => ../..\n' > sdk/go/go.mod
	printf '%s\n' 'module example.invalid/fx/tool' '' 'go 1.22' '' '// in-repo' 'replace (' \
		'	example.invalid/fx => ..' '	example.invalid/fx/sdk/go => ../sdk/go' '	example.invalid/other => example.invalid/fork v1.0.0' ')' '' \
		'require (' '	example.invalid/fx v0.0.1-alpha' '	example.invalid/fx/sdk/go v0.0.0-00010101000000-000000000000' '	example.invalid/other v1.0.0' ')' > tool/go.mod
	git add -A
	git commit -q -m modules
	git push -q origin main
	: > "$DIR/wrapper.log"
}

# publish — commit whatever the case staged and push it, so the tree is clean
# and the remote is level with HEAD before the recipe runs.
publish() { git add -A; git commit -q -m setup; git push -q origin main; }

fragment() { printf '%s\n' "$2" > "changelog.d/$1"; }

release() { make -s release-alpha "$@" >"$ROOT/out" 2>&1; }

# fakego — a go first on PATH ($DIR/fakego) that runs everything for real except
# the template's post-push consumer check (GOWORK=off GOFLAGS=-mod=mod), whose
# commands it records in $DIR/go.log instead of reaching a module proxy.
fakego() {
	local real; real="$(command -v go)" || return 1
	mkdir -p "$DIR/fakego"
	# shellcheck disable=SC2016 # the stub expands its own variables
	printf '%s\n' '#!/bin/sh' \
		'if [ "${GOWORK:-}" = off ] && [ "${GOFLAGS:-}" = -mod=mod ]; then' \
		"	printf '%s\\n' \"\$*\" >> '$DIR/go.log'; exit 0" \
		'fi' \
		"exec '$real' \"\$@\"" > "$DIR/fakego/go"
	chmod +x "$DIR/fakego/go"
	: > "$DIR/go.log"
}

remote_tags() { git ls-remote --tags --refs origin | sed 's|.*refs/tags/||' | sort | tr '\n' ' '; }

# nothing_happened <name> <head> — no commit, no VERSION write, no local tag,
# nothing on the remote.
nothing_happened() {
	local name="$1" head="$2"
	[ "$(git rev-parse HEAD)" = "$head" ] || { bad "$name" "a commit was made"; return 1; }
	[ "$(cat VERSION)" = 0.0.1-alpha ] || { bad "$name" "VERSION was written"; return 1; }
	[ -z "$(git status --porcelain)" ] || { bad "$name" "working tree was modified: $(git status --porcelain | tr '\n' ' ')"; return 1; }
	[ -z "$(git tag --list)" ] || { bad "$name" "a local tag was created"; return 1; }
	[ -z "$(remote_tags)" ] || { bad "$name" "a tag reached the remote"; return 1; }
	[ "$(git ls-remote origin refs/heads/main | cut -f1)" = "$head" ] || { bad "$name" "remote main moved"; return 1; }
}

pass=0; fail=0
tally() { if "$@"; then pass=$((pass+1)); else fail=$((fail+1)); fi; }

echo "=== the version is explicit ==="
no_version() (
	fixture no-version
	fragment 1-a.md "$(printf '### Added\n\n- a')"; publish
	local head; head="$(git rev-parse HEAD)"
	if release; then bad "no VERSION" "the release ran"; return 1; fi
	grep -q 'state the version explicitly' "$ROOT/out" || { bad "no VERSION" "refused for the wrong reason: $(tail -n1 "$ROOT/out")"; return 1; }
	nothing_happened "no VERSION" "$head" || return 1
	[ ! -s "$DIR/wrapper.log" ] || { bad "no VERSION" "the wrapper ran: $(cat "$DIR/wrapper.log")"; return 1; }
	ok "no VERSION refuses before anything is written"
)
tally no_version

bad_version() (
	fixture bad-version
	fragment 1-a.md "$(printf '### Added\n\n- a')"; publish
	local head; head="$(git rev-parse HEAD)"
	if release VERSION=v0.1.0; then bad "VERSION without -alpha" "the release ran"; return 1; fi
	grep -q 'not of the form' "$ROOT/out" || { bad "VERSION without -alpha" "refused for the wrong reason: $(tail -n1 "$ROOT/out")"; return 1; }
	nothing_happened "VERSION without -alpha" "$head" || return 1
	ok "a VERSION outside vX.Y.Z-alpha refuses before anything is written"
)
tally bad_version

echo "=== a full release: wrapper, annotated tag, assembled CHANGELOG ==="
full_release() (
	local name="full release"
	fixture full
	printf '# Changelog\n\n## [Unreleased]\n\n### Fixed\n\n- unreleased fix\n\n## [0.0.1-alpha] - 2026-01-01\n\n### Added\n\n- The first release.\n' > CHANGELOG.md
	# Byte order: 10-b.md sorts before 2-a.md, so its Added heading comes first.
	fragment 10-b.md "$(printf '### Added\n\n- fragment b added')"
	fragment 2-a.md "$(printf '### Changed\n\n- fragment a changed\n\n### Added\n\n- fragment a added')"
	publish
	if ! release VERSION="$VER"; then bad "$name" "recipe refused: $(tail -n3 "$ROOT/out")"; return 1; fi

	# Wrapper: every git write, in order, as go-objstore, with these arguments.
	local want
	want="$(printf '%s\n' \
		"go-objstore add CHANGELOG.md" \
		"go-objstore rm -q -- changelog.d/10-b.md changelog.d/2-a.md" \
		"go-objstore commit -m docs(changelog): assemble $PLAIN for $VER" \
		"go-objstore add -- VERSION" \
		"go-objstore add -u" \
		"go-objstore commit -m chore(release): $VER" \
		"go-objstore tag -a $VER -m $VER" \
		"go-objstore push --atomic --no-follow-tags origin HEAD refs/tags/$VER")"
	[ "$(cat "$DIR/wrapper.log")" = "$want" ] || { bad "$name" "wrapper calls were:
$(cat "$DIR/wrapper.log")
want:
$want"; return 1; }
	ok "every git write goes through the wrapper as go-objstore, with the contract's arguments"

	# Annotated, and the only tag.
	[ "$(remote_tags)" = "$VER " ] || { bad "$name" "remote tags are [$(remote_tags)]"; return 1; }
	git fetch -q origin "refs/tags/$VER:refs/tags/fetched" || { bad "$name" "could not fetch the tag"; return 1; }
	[ "$(git cat-file -t refs/tags/fetched)" = tag ] || { bad "$name" "the published tag is $(git cat-file -t refs/tags/fetched), not an annotated tag object"; return 1; }
	[ "$(git ls-remote origin refs/heads/main | cut -f1)" = "$(git rev-parse HEAD)" ] || { bad "$name" "remote main is not the release commit"; return 1; }
	ok "the published tag is annotated and is the only tag pushed"

	# The TAGGED tree carries the assembled CHANGELOG, VERSION and no fragments.
	local cl; cl="$(git show "refs/tags/fetched:CHANGELOG.md")"
	local expect_body
	expect_body="$(printf '%s\n' \
		"### Fixed" "" "- unreleased fix" "" \
		"### Added" "" "- fragment b added" "" "- fragment a added" "" \
		"### Changed" "" "- fragment a changed" "" \
		"## [0.0.1-alpha] - 2026-01-01")"
	printf '%s\n' "$cl" | sed -n '3,5p' | tr '\n' '|' | grep -Eq "^## \[Unreleased\]\|\|## \[$PLAIN\] - $TODAY_RE\|$" \
		|| { bad "$name" "the tagged CHANGELOG does not open with an empty Unreleased section then [$PLAIN]:
$cl"; return 1; }
	[ "$(printf '%s\n' "$cl" | sed -n '7,/^## \[0\.0\.1-alpha\]/p')" = "$expect_body" ] || { bad "$name" "the tagged CHANGELOG section is:
$(printf '%s\n' "$cl" | sed -n '7,/^## \[0\.0\.1-alpha\]/p')
want:
$expect_body"; return 1; }
	[ "$(git show refs/tags/fetched:VERSION)" = "$PLAIN" ] || { bad "$name" "the tagged VERSION is $(git show refs/tags/fetched:VERSION)"; return 1; }
	[ "$(git ls-tree --name-only refs/tags/fetched changelog.d/)" = changelog.d/.gitkeep ] \
		|| { bad "$name" "the tagged changelog.d/ holds: $(git ls-tree --name-only refs/tags/fetched changelog.d/ | tr '\n' ' ')"; return 1; }
	ok "the tagged tree holds the assembled section, grouped by subsection, and no consumed fragment"
)
tally full_release

echo "=== the release commit carries the synced SDK versions ==="
# The real Makefile, scripts and SDK manifests (copied from this checkout) run
# in the fixture, so version-sync and version-check are the repo's own targets.
# Every manifest starts at the checkout's VERSION; the release must move all of
# them to the new version inside the one signed release commit, leaving
# version-check green on that commit (v0.4.0-alpha left main red).
SDK_FILES="api/sdks/go/version.go api/sdks/python/setup.py api/sdks/python/pyproject.toml
api/sdks/python/objstore/__init__.py api/sdks/ruby/lib/objstore/version.rb
api/sdks/ruby/objstore.gemspec api/sdks/rust/Cargo.toml api/sdks/typescript/package.json
api/sdks/typescript/package-lock.json api/sdks/csharp/ObjStore.SDK.csproj"
# The go.mod files of the root and the nested modules released in lockstep.
MOD_FILES="go.mod backup/go.mod api/sdks/go/go.mod"
sdk_lockstep() (
	local name="sdk lockstep" repo f m
	repo="$(cd "$HERE/../.." && pwd)"
	fixture sdk-lockstep
	export AGENT_GIT="$DIR/agent-git"
	# The real Makefile. Only the post-push consumer check's go commands are
	# stubbed (fakego): they resolve the tags through the module proxy, which a
	# throwaway remote is not.
	cp "$repo/Makefile" Makefile
	fakego || { bad "$name" "go is not on PATH"; return 1; }
	export PATH="$DIR/fakego:$PATH"
	cp "$repo/VERSION" VERSION
	# The next minor above the real VERSION: a release never lowers it. This
	# function runs in a subshell, so the shadowed VER stays here.
	local maj min
	IFS=. read -r maj min _ < VERSION
	VER="v$maj.$((min + 1)).0-alpha"
	mkdir -p mk/template scripts
	cp "$repo"/mk/*.mk "$repo"/mk/*.sh mk/
	cp "$repo"/mk/template/*.mk mk/template/
	cp "$repo/scripts/sync-version.sh" "$repo/scripts/check-version.sh" scripts/
	for f in $SDK_FILES $MOD_FILES; do mkdir -p "$(dirname "$f")"; cp "$repo/$f" "$f"; done
	make -s version-check >/dev/null 2>&1 || { bad "$name" "the fixture does not start with matching SDK versions"; return 1; }
	fragment 1-a.md "$(printf '### Added\n\n- a')"; publish
	: > "$DIR/wrapper.log"
	if ! release VERSION="$VER"; then bad "$name" "recipe refused: $(tail -n3 "$ROOT/out")"; return 1; fi
	for m in github.com/jeremyhahn/go-objstore github.com/jeremyhahn/go-objstore/backup github.com/jeremyhahn/go-objstore/api/sdks/go; do
		grep -qx "get $m@$VER" "$DIR/go.log" || { bad "$name" "the consumer check did not get $m@$VER after the push: $(tr '\n' ' ' < "$DIR/go.log")"; return 1; }
	done

	# One release commit, signed through the wrapper, holding VERSION, every
	# manifest and the re-pinned nested go.mod files; then the restore commit.
	grep -qx "go-objstore add -u" "$DIR/wrapper.log" || { bad "$name" "the synced manifests were never staged: $(cat "$DIR/wrapper.log")"; return 1; }
	[ "$(git log -1 --format=%s HEAD^)" = "chore(release): $VER" ] || { bad "$name" "HEAD^ is not the release commit: $(git log -1 --format=%s HEAD^)"; return 1; }
	local changed; changed="$(git diff-tree --no-commit-id --name-only -r HEAD^ | sort)"
	local want; want="$(printf '%s\n' VERSION $SDK_FILES backup/go.mod api/sdks/go/go.mod | sort)"
	[ "$changed" = "$want" ] || { bad "$name" "the release commit changed:
$changed
want:
$want"; return 1; }
	[ "$(git diff-tree --no-commit-id --name-only -r HEAD | sort | tr '\n' ' ')" = "api/sdks/go/go.mod backup/go.mod " ] \
		|| { bad "$name" "the restore commit changed: $(git diff-tree --no-commit-id --name-only -r HEAD | tr '\n' ' ')"; return 1; }
	[ -z "$(git status --porcelain)" ] || { bad "$name" "the working tree is dirty after the release: $(git status --porcelain | tr '\n' ' ')"; return 1; }
	ok "the release commit holds VERSION, all ten SDK manifests and both nested go.mod files; the tree ends clean"

	# The tagged go.mod files: own-module requirements at the release version, no
	# replace directive left; on main the in-repo replaces are back.
	local mod
	for mod in backup/go.mod api/sdks/go/go.mod; do
		git show "HEAD^:$mod" > "$ROOT/at" || { bad "$name" "no $mod at the tag"; return 1; }
		! grep -q '=>' "$ROOT/at" || { bad "$name" "$mod keeps a replace at the tag"; return 1; }
		[ "$(grep -Ec "github\.com/jeremyhahn/go-objstore(/api/sdks/go)? $VER( //.*)?\$" "$ROOT/at")" = "$([ "$mod" = backup/go.mod ] && echo 2 || echo 1)" ] \
			|| { bad "$name" "$mod does not require the repo's own modules at $VER: $(grep jeremyhahn/go-objstore "$ROOT/at" | tr '\n' ' ')"; return 1; }
		grep -q '=> \.\.' "$mod" || { bad "$name" "$mod lost its in-repo replace on main"; return 1; }
	done
	ok "the nested go.mod files require $VER without replaces at the tag and keep them on main"

	# The published tags name the release commit, and its tree passes the gate.
	local t
	for t in "$VER" "backup/$VER" "api/sdks/go/$VER"; do
		[ "$(git ls-remote origin "refs/tags/$t^{}" | cut -f1)" = "$(git rev-parse HEAD^)" ] || { bad "$name" "tag $t is not the release commit"; return 1; }
	done
	git fetch -q origin "refs/tags/$VER:refs/tags/fetched" || { bad "$name" "could not fetch the tag"; return 1; }
	git checkout -q --detach refs/tags/fetched || { bad "$name" "could not check out the tag"; return 1; }
	make -s version-check >"$ROOT/out" 2>&1 || { bad "$name" "version-check fails on the release commit: $(tail -n5 "$ROOT/out")"; return 1; }
	ok "the three tags name the release commit, and version-check passes on it"
)
tally sdk_lockstep

echo "=== a failed commit stops the release ==="
# The stub fails the first commit it is asked for; no tag may exist anywhere
# and the remote must not move.
failed_assembly_commit() (
	local name="a failed CHANGELOG assembly commit"
	fixture fail-assembly
	fragment 1-a.md "$(printf '### Added\n\n- a')"; publish
	local head; head="$(git rev-parse HEAD)"
	if FAIL_ON=commit release VERSION="$VER"; then bad "$name" "the release ran"; return 1; fi
	[ "$(git rev-parse HEAD)" = "$head" ] || { bad "$name" "a commit was made"; return 1; }
	[ -z "$(git tag --list)" ] || { bad "$name" "a local tag was created"; return 1; }
	[ -z "$(remote_tags)" ] || { bad "$name" "a tag reached the remote"; return 1; }
	[ "$(git ls-remote origin refs/heads/main | cut -f1)" = "$head" ] || { bad "$name" "remote main moved"; return 1; }
	! grep -Eq ' (tag|push) ' "$DIR/wrapper.log" || { bad "$name" "tag or push was attempted: $(cat "$DIR/wrapper.log")"; return 1; }
	ok "$name creates no tag and pushes nothing"
)
tally failed_assembly_commit

failed_version_commit() (
	local name="a failed VERSION commit"
	fixture fail-version
	# Already assembled by hand: the assembly step is a no-op, so the first
	# commit the wrapper sees is the template's VERSION commit.
	printf '# Changelog\n\n## [Unreleased]\n\n## [%s] - 2026-02-02\n\n### Added\n\n- b\n' "$PLAIN" > CHANGELOG.md
	publish
	local head; head="$(git rev-parse HEAD)"
	if FAIL_ON=commit release VERSION="$VER"; then bad "$name" "the release ran"; return 1; fi
	grep -q "^go-objstore commit -m chore(release): $VER\$" "$DIR/wrapper.log" || { bad "$name" "the VERSION commit was never attempted: $(cat "$DIR/wrapper.log")"; return 1; }
	[ "$(git rev-parse HEAD)" = "$head" ] || { bad "$name" "a commit was made"; return 1; }
	[ -z "$(git tag --list)" ] || { bad "$name" "a tag was created after the commit failed"; return 1; }
	[ -z "$(remote_tags)" ] || { bad "$name" "a tag reached the remote"; return 1; }
	[ "$(git ls-remote origin refs/heads/main | cut -f1)" = "$head" ] || { bad "$name" "remote main moved"; return 1; }
	! grep -Eq ' (tag|push) ' "$DIR/wrapper.log" || { bad "$name" "tag or push was attempted: $(cat "$DIR/wrapper.log")"; return 1; }
	ok "$name creates no tag and pushes nothing (the v1 recipe's || true tagged anyway)"
)
tally failed_version_commit

echo "=== CHANGELOG assembly guards ==="
hand_prepared() (
	local name="a hand-prepared section with nothing pending"
	fixture hand
	printf '# Changelog\n\n## [Unreleased]\n\n## [%s] - 2026-02-02\n\n### Added\n\n- b\n' "$PLAIN" > CHANGELOG.md
	publish
	local before; before="$(cat CHANGELOG.md)"
	if ! release VERSION="$VER"; then bad "$name" "recipe refused: $(tail -n3 "$ROOT/out")"; return 1; fi
	[ "$(git show "$VER:CHANGELOG.md")" = "$before" ] || { bad "$name" "the CHANGELOG was rewritten"; return 1; }
	! grep -q 'docs(changelog)' "$DIR/wrapper.log" || { bad "$name" "an assembly commit was made"; return 1; }
	ok "$name releases as-is, with no assembly commit"
)
tally hand_prepared

duplicate_section() (
	local name="a version section that already exists while fragments are pending"
	fixture duplicate
	printf '# Changelog\n\n## [Unreleased]\n\n## [%s] - 2026-02-02\n\n### Added\n\n- b\n' "$PLAIN" > CHANGELOG.md
	fragment 1-a.md "$(printf '### Added\n\n- a')"; publish
	local head; head="$(git rev-parse HEAD)"
	if release VERSION="$VER"; then bad "$name" "the release ran"; return 1; fi
	grep -q 'assembling would duplicate it' "$ROOT/out" || { bad "$name" "refused for the wrong reason: $(tail -n1 "$ROOT/out")"; return 1; }
	nothing_happened "$name" "$head" || return 1
	ok "$name refuses before anything is written"
)
tally duplicate_section

nothing_pending() (
	local name="no fragments, an empty Unreleased section and no version section"
	fixture empty
	local head; head="$(git rev-parse HEAD)"
	if release VERSION="$VER"; then bad "$name" "the release ran"; return 1; fi
	grep -q 'nothing to release' "$ROOT/out" || { bad "$name" "refused for the wrong reason: $(tail -n1 "$ROOT/out")"; return 1; }
	nothing_happened "$name" "$head" || return 1
	ok "$name refuses before anything is written"
)
tally nothing_pending

wrong_branch() (
	local name="a release from a branch other than main"
	fixture branch
	fragment 1-a.md "$(printf '### Added\n\n- a')"; publish
	git checkout -q -b feature
	local head; head="$(git rev-parse HEAD)"
	if release VERSION="$VER"; then bad "$name" "the release ran"; return 1; fi
	grep -q 'releases are cut from main' "$ROOT/out" || { bad "$name" "refused for the wrong reason: $(tail -n1 "$ROOT/out")"; return 1; }
	nothing_happened "$name" "$head" || return 1
	ok "$name refuses before the assembly commit"
)
tally wrong_branch

existing_remote_tag() (
	local name="a tag that already exists on the remote"
	fixture remote-tag
	fragment 1-a.md "$(printf '### Added\n\n- a')"; publish
	git tag -a "$VER" -m x && git push -q origin "refs/tags/$VER" && git tag -d "$VER" >/dev/null
	local head; head="$(git rev-parse HEAD)"
	if release VERSION="$VER"; then bad "$name" "the release ran"; return 1; fi
	grep -q "already exists on origin" "$ROOT/out" || { bad "$name" "refused for the wrong reason: $(tail -n1 "$ROOT/out")"; return 1; }
	[ "$(git rev-parse HEAD)" = "$head" ] || { bad "$name" "an assembly commit was made"; return 1; }
	[ -z "$(git status --porcelain)" ] || { bad "$name" "working tree was modified"; return 1; }
	ok "$name refuses before the assembly commit"
)
tally existing_remote_tag

fenced_headings() (
	local name="headings inside a fenced block are content, not subsections"
	fixture fenced
	# shellcheck disable=SC2016 # the backticks are a literal Markdown fence
	fragment 1-a.md "$(printf '### Added\n\n- a, for example:\n\n```\n### Removed\n## [9.9.9]\n```')"
	publish
	if ! release VERSION="$VER"; then bad "$name" "recipe refused: $(tail -n3 "$ROOT/out")"; return 1; fi
	local got want
	got="$(git show "$VER:CHANGELOG.md" | sed -n '7,$p')"
	want="$(printf '%s\n' "### Added" "" "- a, for example:" "" '```' "### Removed" "## [9.9.9]" '```' "" "## [0.0.1-alpha] - 2026-01-01" "" "### Added" "" "- The first release.")"
	[ "$got" = "$want" ] || { bad "$name" "the tagged section is:
$got
want:
$want"; return 1; }
	ok "$name"
)
tally fenced_headings

echo "=== an empty Unreleased section directly above the previous version ==="
# "## [Unreleased]" immediately followed by the previous version's heading, no
# blank line between: the Unreleased body is empty and must assemble to
# nothing — never to a copy of the heading that follows it.
ADJACENT='# Changelog\n\n## [Unreleased]\n## [0.0.1-alpha] - 2026-01-01\n\n### Added\n\n- The first release.\n'

adjacent_with_fragment() (
	local name="adjacent headings with a pending fragment"
	fixture adjacent-fragment
	# shellcheck disable=SC2059 # ADJACENT is the format on purpose
	printf "$ADJACENT" > CHANGELOG.md
	fragment 1-a.md "$(printf '### Added\n\n- a')"; publish
	if ! release VERSION="$VER"; then bad "$name" "recipe refused: $(tail -n3 "$ROOT/out")"; return 1; fi
	local cl got want
	cl="$(git show "$VER:CHANGELOG.md")"
	[ "$(printf '%s\n' "$cl" | grep -c '^## \[0\.0\.1-alpha\]')" -eq 1 ] || { bad "$name" "the previous version heading is duplicated:
$cl"; return 1; }
	printf '%s\n' "$cl" | sed -n '3,5p' | tr '\n' '|' | grep -Eq "^## \[Unreleased\]\|\|## \[$PLAIN\] - $TODAY_RE\|$" \
		|| { bad "$name" "the CHANGELOG does not open with an empty Unreleased section then [$PLAIN]:
$cl"; return 1; }
	got="$(printf '%s\n' "$cl" | sed -n '6,$p')"
	want="$(printf '%s\n' "" "### Added" "" "- a" "" "## [0.0.1-alpha] - 2026-01-01" "" "### Added" "" "- The first release.")"
	[ "$got" = "$want" ] || { bad "$name" "the tagged CHANGELOG after the new heading is:
$got
want:
$want"; return 1; }
	ok "$name assemble only the fragment, under the new version"
)
tally adjacent_with_fragment

adjacent_nothing_pending() (
	local name="adjacent headings with nothing pending and no version section"
	fixture adjacent-empty
	# shellcheck disable=SC2059 # ADJACENT is the format on purpose
	printf "$ADJACENT" > CHANGELOG.md
	publish
	local head; head="$(git rev-parse HEAD)"
	if release VERSION="$VER"; then bad "$name" "the release ran"; return 1; fi
	grep -q 'nothing to release' "$ROOT/out" || { bad "$name" "refused for the wrong reason: $(tail -n1 "$ROOT/out")"; return 1; }
	nothing_happened "$name" "$head" || return 1
	ok "$name refuse as nothing to release"
)
tally adjacent_nothing_pending

adjacent_hand_prepared() (
	local name="adjacent headings above a hand-prepared version section"
	fixture adjacent-hand
	printf '# Changelog\n\n## [Unreleased]\n## [%s] - 2026-02-02\n\n### Added\n\n- b\n' "$PLAIN" > CHANGELOG.md
	publish
	local before; before="$(cat CHANGELOG.md)"
	if ! release VERSION="$VER"; then bad "$name" "recipe refused: $(tail -n3 "$ROOT/out")"; return 1; fi
	[ "$(git show "$VER:CHANGELOG.md")" = "$before" ] || { bad "$name" "the CHANGELOG was rewritten"; return 1; }
	! grep -q 'docs(changelog)' "$DIR/wrapper.log" || { bad "$name" "an assembly commit was made"; return 1; }
	ok "$name release as-is, with no assembly commit"
)
tally adjacent_hand_prepared

echo "=== the lockstep guard runs before assembly ==="
# SUBMODULE_TAGS set: release-alpha must refuse, and under make -j2 the
# assembly step must not run beside the failing lockstep check.
lockstep_parallel() (
	local name="a bad LOCKSTEP_MODULES entry under make -j2"
	nested lockstep
	fragment 1-a.md "$(printf '### Added\n\n- a')"; publish
	local head; head="$(git rev-parse HEAD)"
	: > "$DIR/wrapper.log"
	if make -s -j2 release-alpha VERSION="$VER" LOCKSTEP_MODULES="sdk/go ../escape" >"$ROOT/out" 2>&1; then bad "$name" "the release ran"; return 1; fi
	grep -q 'not a plain repo-relative directory: ../escape' "$ROOT/out" || { bad "$name" "refused for the wrong reason: $(tail -n1 "$ROOT/out")"; return 1; }
	nothing_happened "$name" "$head" || return 1
	[ -f changelog.d/1-a.md ] || { bad "$name" "the fragment was consumed"; return 1; }
	[ ! -s "$DIR/wrapper.log" ] || { bad "$name" "the wrapper ran: $(cat "$DIR/wrapper.log")"; return 1; }
	# Deterministic form of the same ordering: assembly alone must refuse too,
	# here for a module directory with no go.mod.
	if make -s -j2 changelog-assemble VERSION="$VER" LOCKSTEP_MODULES="sdk/go nope" >"$ROOT/out" 2>&1; then bad "$name" "changelog-assemble ran without the lockstep check"; return 1; fi
	grep -q 'entry nope has no tracked go.mod' "$ROOT/out" || { bad "$name" "refused for the wrong reason: $(tail -n1 "$ROOT/out")"; return 1; }
	nothing_happened "$name" "$head" || return 1
	[ ! -s "$DIR/wrapper.log" ] || { bad "$name" "the wrapper ran: $(cat "$DIR/wrapper.log")"; return 1; }
	ok "$name leaves HEAD, the working tree and the remote unchanged"
)

# A nested module's tag already on the remote, or only local: refused before the
# assembly commit, so no CHANGELOG change is ever committed for it.
nested_tag_exists() (
	local name="a nested module tag that already exists" where
	for where in remote local; do
		nested "nested-tag-$where"
		fragment 1-a.md "$(printf '### Added\n\n- a')"; publish
		git tag -a "tool/$VER" -m x || { bad "$name" "fixture tag"; return 1; }
		if [ "$where" = remote ]; then git push -q origin "refs/tags/tool/$VER" && git tag -d "tool/$VER" >/dev/null; fi
		local head; head="$(git rev-parse HEAD)"
		: > "$DIR/wrapper.log"
		if release VERSION="$VER"; then bad "$name ($where)" "the release ran"; return 1; fi
		local want="tag tool/$VER already exists locally"; [ "$where" = local ] || want="tag tool/$VER already exists on origin"
		grep -q "$want" "$ROOT/out" || { bad "$name ($where)" "refused for the wrong reason: $(tail -n1 "$ROOT/out")"; return 1; }
		[ "$(git rev-parse HEAD)" = "$head" ] || { bad "$name ($where)" "an assembly commit was made"; return 1; }
		[ -z "$(git status --porcelain)" ] || { bad "$name ($where)" "working tree was modified"; return 1; }
		[ ! -s "$DIR/wrapper.log" ] || { bad "$name ($where)" "the wrapper ran: $(cat "$DIR/wrapper.log")"; return 1; }
		[ "$(git ls-remote origin "refs/tags/$VER" "refs/tags/sdk/go/$VER")" = "" ] || { bad "$name ($where)" "a release tag reached the remote"; return 1; }
	done
	ok "$name, locally or on the remote, is refused before the assembly commit"
)

# The lockstep release: three tags on one release commit whose nested go.mod
# files require the root and SDK at the new version with no in-repo replace,
# then a second commit restoring the replaces, all in one atomic push.
nested_release() (
	local name="a lockstep release of the nested modules"
	command -v go >/dev/null 2>&1 || { bad "$name" "go is not on PATH"; return 1; }
	nested nested-release
	fragment 1-a.md "$(printf '### Added\n\n- a')"; publish
	: > "$DIR/wrapper.log"
	fakego || { bad "$name" "go is not on PATH"; return 1; }
	if ! PATH="$DIR/fakego:$PATH" release VERSION="$VER" RELEASE_REPLACES="sdk/go tool"; then bad "$name" "recipe refused: $(tail -n3 "$ROOT/out")"; return 1; fi
	grep -qx "get example.invalid/fx@$VER" "$DIR/go.log" || { bad "$name" "the consumer check of the root did not run: $(tr '\n' ' ' < "$DIR/go.log")"; return 1; }

	local rel; rel="$(git rev-parse HEAD^)"
	[ "$(git log -1 --format=%s HEAD^)" = "chore(release): $VER" ] || { bad "$name" "HEAD^ is not the release commit: $(git log -1 --format=%s HEAD^)"; return 1; }
	[ "$(git log -1 --format=%s)" = "chore(release): restore in-repo replaces after $VER" ] || { bad "$name" "HEAD is not the restore commit: $(git log -1 --format=%s)"; return 1; }
	local t
	for t in "$VER" "sdk/go/$VER" "tool/$VER"; do
		[ "$(git ls-remote origin "refs/tags/$t^{}" | cut -f1)" = "$rel" ] || { bad "$name" "remote tag $t does not name the release commit"; return 1; }
	done
	[ "$(remote_tags)" = "sdk/go/$VER tool/$VER $VER " ] || { bad "$name" "the remote has other tags: $(remote_tags)"; return 1; }
	[ "$(git ls-remote origin refs/heads/main | cut -f1)" = "$(git rev-parse HEAD)" ] || { bad "$name" "remote main is not the restore commit"; return 1; }
	[ "$(grep -c '^go-objstore push ' "$DIR/wrapper.log")" -eq 1 ] || { bad "$name" "not exactly one push: $(cat "$DIR/wrapper.log")"; return 1; }
	grep -qx "go-objstore push --atomic --no-follow-tags origin HEAD refs/tags/$VER refs/tags/sdk/go/$VER refs/tags/tool/$VER" "$DIR/wrapper.log" \
		|| { bad "$name" "unexpected push: $(grep push "$DIR/wrapper.log")"; return 1; }
	ok "three tags on the release commit, the restore commit on main, one atomic push"

	local at; at="$(git show "$rel:tool/go.mod")"
	printf '%s\n' "$at" | grep -q '=> \.\.' && { bad "$name" "tool/go.mod keeps an in-repo replace at the tag:
$at"; return 1; }
	printf '%s\n' "$at" | grep -Eq "^[[:space:]]*(require )?example\.invalid/fx $VER\$" || { bad "$name" "tool/go.mod does not require the root at $VER:
$at"; return 1; }
	printf '%s\n' "$at" | grep -Eq "^[[:space:]]*(require )?example\.invalid/fx/sdk/go $VER\$" || { bad "$name" "tool/go.mod does not require the SDK at $VER:
$at"; return 1; }
	printf '%s\n' "$at" | grep -q 'example.invalid/other => example.invalid/fork v1.0.0' || { bad "$name" "a third-party replace was dropped:
$at"; return 1; }
	at="$(git show "$rel:sdk/go/go.mod")"
	printf '%s\n' "$at" | grep -q '^replace' && { bad "$name" "sdk/go/go.mod keeps its replace at the tag:
$at"; return 1; }
	printf '%s\n' "$at" | grep -Eq "^require example\.invalid/fx $VER\$" || { bad "$name" "sdk/go/go.mod does not require the root at $VER:
$at"; return 1; }
	[ "$(git diff-tree --no-commit-id --name-only -r "$rel" | sort | tr '\n' ' ')" = "VERSION sdk/go/go.mod tool/go.mod " ] \
		|| { bad "$name" "the release commit changed: $(git diff-tree --no-commit-id --name-only -r "$rel" | tr '\n' ' ')"; return 1; }
	ok "the tagged go.mod files require the release version and drop only the in-repo replaces"

	[ "$(git diff-tree --no-commit-id --name-only -r HEAD | sort | tr '\n' ' ')" = "sdk/go/go.mod tool/go.mod " ] \
		|| { bad "$name" "the restore commit changed: $(git diff-tree --no-commit-id --name-only -r HEAD | tr '\n' ' ')"; return 1; }
	at="$(cat tool/go.mod)"
	printf '%s\n' "$at" | grep -q '// in-repo' || { bad "$name" "the restored tool/go.mod lost its comment:
$at"; return 1; }
	printf '%s\n' "$at" | grep -q 'example.invalid/fx/sdk/go => ../sdk/go' || { bad "$name" "the SDK replace was not restored:
$at"; return 1; }
	printf '%s\n' "$at" | grep -Eq "^[[:space:]]*example\.invalid/fx/sdk/go $VER\$" || { bad "$name" "the restored SDK requirement is not $VER:
$at"; return 1; }
	grep -q '^replace example.invalid/fx => ../..$' sdk/go/go.mod || { bad "$name" "the root replace in sdk/go was not restored"; return 1; }
	grep -Eq "^require example\.invalid/fx $VER\$" sdk/go/go.mod || { bad "$name" "the restored root requirement in sdk/go is not $VER"; return 1; }
	[ "$(cat VERSION)" = "$PLAIN" ] || { bad "$name" "VERSION is $(cat VERSION) after the restore"; return 1; }
	[ -z "$(git status --porcelain)" ] || { bad "$name" "the working tree is dirty: $(git status --porcelain | tr '\n' ' ')"; return 1; }
	[ ! -e "$(git rev-parse --git-dir)/release-unpinned" ] || { bad "$name" "the drop record was left behind"; return 1; }
	ok "main keeps the in-repo replaces, required at $VER, with a clean tree"
)
tally lockstep_parallel
tally nested_tag_exists
tally nested_release

echo "=== inputs are data, never recipe text ==="
no_injection() (
	local name="a \$(shell ...) payload in any release input does not execute"
	fixture injection
	fragment 1-a.md "$(printf '### Added\n\n- a')"; publish
	local head var; head="$(git rev-parse HEAD)"
	for var in VERSION RELEASE_REMOTE REPO_NAME AGENT_GIT LOCKSTEP_MODULES CL_VERSION CL_REMOTE CL_REPO CL_AGENT_GIT CL_MODULES RELEASE_MK_DIR; do
		# shellcheck disable=SC2016 # the payload must reach make unexpanded
		make -s release-alpha "$var=\$(shell touch $DIR/fired-$var)x" >/dev/null 2>&1
		[ ! -e "$DIR/fired-$var" ] || { bad "$name" "$var executed its payload"; return 1; }
	done
	# The template's opt-in inputs are not changelog-assemble's, so the assembly
	# commits before the template refuses them: undo it after each.
	for var in RELEASE_REPLACES RELEASE_SYNC_TARGETS RELEASE_CONSUMER_MODULES REL_REPLACES REL_SYNC_TARGETS REL_CONSUMER_MODULES REL_MAKE; do
		# shellcheck disable=SC2016 # the payload must reach make unexpanded
		make -s release-alpha "$var=\$(shell touch $DIR/fired-$var)x" >/dev/null 2>&1
		[ ! -e "$DIR/fired-$var" ] || { bad "$name" "$var executed its payload"; return 1; }
		git reset -q --hard "$head"
	done
	nothing_happened "$name" "$head" || return 1
	ok "$name, and nothing is written"
)
tally no_injection

echo
echo "release path: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
