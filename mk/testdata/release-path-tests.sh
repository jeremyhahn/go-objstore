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
include $WIRING
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

# publish — commit whatever the case staged and push it, so the tree is clean
# and the remote is level with HEAD before the recipe runs.
publish() { git add -A; git commit -q -m setup; git push -q origin main; }

fragment() { printf '%s\n' "$2" > "changelog.d/$1"; }

release() { make -s release-alpha "$@" >"$ROOT/out" 2>&1; }

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
		"go-objstore add VERSION" \
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
	local name="a lockstep rejection under make -j2"
	fixture lockstep
	fragment 1-a.md "$(printf '### Added\n\n- a')"; publish
	local head; head="$(git rev-parse HEAD)"
	if make -s -j2 release-alpha VERSION="$VER" SUBMODULE_TAGS=sdk/go >"$ROOT/out" 2>&1; then bad "$name" "the release ran"; return 1; fi
	grep -q 'SUBMODULE_TAGS is set' "$ROOT/out" || { bad "$name" "refused for the wrong reason: $(tail -n1 "$ROOT/out")"; return 1; }
	nothing_happened "$name" "$head" || return 1
	[ -f changelog.d/1-a.md ] || { bad "$name" "the fragment was consumed"; return 1; }
	[ ! -s "$DIR/wrapper.log" ] || { bad "$name" "the wrapper ran: $(cat "$DIR/wrapper.log")"; return 1; }
	# Deterministic form of the same ordering: assembly alone must refuse too.
	if make -s -j2 changelog-assemble VERSION="$VER" SUBMODULE_TAGS=sdk/go >"$ROOT/out" 2>&1; then bad "$name" "changelog-assemble ran without the lockstep check"; return 1; fi
	nothing_happened "$name" "$head" || return 1
	[ ! -s "$DIR/wrapper.log" ] || { bad "$name" "the wrapper ran: $(cat "$DIR/wrapper.log")"; return 1; }
	ok "$name leaves HEAD, the working tree and the remote unchanged"
)
tally lockstep_parallel

echo "=== inputs are data, never recipe text ==="
no_injection() (
	local name="a \$(shell ...) payload in any release input does not execute"
	fixture injection
	fragment 1-a.md "$(printf '### Added\n\n- a')"; publish
	local head var; head="$(git rev-parse HEAD)"
	for var in VERSION RELEASE_REMOTE REPO_NAME AGENT_GIT CL_VERSION CL_REMOTE CL_REPO CL_AGENT_GIT RELEASE_MK_DIR; do
		# shellcheck disable=SC2016 # the payload must reach make unexpanded
		make -s release-alpha "$var=\$(shell touch $DIR/fired-$var)x" >/dev/null 2>&1
		[ ! -e "$DIR/fired-$var" ] || { bad "$name" "$var executed its payload"; return 1; }
	done
	nothing_happened "$name" "$head" || return 1
	ok "$name, and nothing is written"
)
tally no_injection

echo
echo "release path: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
