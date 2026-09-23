#!/usr/bin/env bash
# changelog-assemble.sh — folds changelog.d/ fragments and the existing
# "## [Unreleased]" content into a new "## [X.Y.Z-alpha] - YYYY-MM-DD" section of
# CHANGELOG.md, deletes the consumed fragments, and commits the result through
# the signing wrapper. release-versioning.md §5: assembly is a COMMITTED step
# performed BEFORE tagging, so the tag's tree carries the assembled entry.
#
# Run by `make changelog-assemble`, a prerequisite of `make release-alpha`
# (mk/release.mk). Inputs arrive as environment variables, set by that target
# through the same override/export/$(value) pattern the release template uses:
#
#   CL_VERSION    vX.Y.Z-alpha, exactly as given to release-alpha
#   CL_AGENT_GIT  the agent-git.sh wrapper; every git write goes through it
#   CL_REPO       the literal repo name the wrapper signs as
#   CL_REMOTE     the Gitea remote the release is pushed to
#
# The pre-flight guards mirror the template's, so an assembly commit is never
# made for a release the template would then refuse. Every failure exits
# non-zero before CHANGELOG.md is touched.
#
# Idempotent: with no fragments and an empty Unreleased section, a CHANGELOG
# that already has the version's section is left alone (hand-prepared, or a
# release re-run after a later step failed) — the template's guard then checks it.
set -euo pipefail

die() { printf 'ERROR: %s\n' "$1" >&2; exit 1; }

ver="${CL_VERSION:-}"
[ -n "$ver" ] || die 'state the version explicitly: make release-alpha VERSION=vX.Y.Z-alpha (release-versioning.md decides patch/minor/major — the recipe refuses to guess)'
printf '%s' "$ver" | grep -Eq '^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)-alpha$' \
	|| die "VERSION is not of the form vX.Y.Z-alpha (no leading zeroes): $ver"
branch="$(git rev-parse --abbrev-ref HEAD)"
[ "$branch" = main ] || die "releases are cut from main; you are on $branch. The CHANGELOG assembly commit would land on this branch"
[ -n "${CL_REPO:-}" ] || die 'REPO_NAME is empty — set it to the literal repo name'
[ -x "${CL_AGENT_GIT:-}" ] || die 'AGENT_GIT does not point at an executable agent-git.sh — the CHANGELOG assembly commit signs through it'
remote="${CL_REMOTE:-}"
[ -n "$remote" ] || die 'RELEASE_REMOTE is empty — set it to the Gitea remote'
git remote get-url "$remote" >/dev/null 2>&1 || die "no git remote named $remote"
! git rev-parse -q --verify "refs/tags/$ver" >/dev/null || die "tag $ver already exists locally"
rc=0; git ls-remote --exit-code --tags "$remote" "refs/tags/$ver" >/dev/null 2>&1 || rc=$?
case "$rc" in
	0) die "tag $ver already exists on $remote" ;;
	2) : ;;
	*) die "could not reach $remote to check whether tag $ver exists. Refusing to continue: an unreachable remote is not an absent tag" ;;
esac
[ -z "$(git status --porcelain)" ] || die 'working tree not clean'
[ -f CHANGELOG.md ] || die 'no CHANGELOG.md'

plain="${ver#v}"
heading="## [$plain]"

# fence_awk — CommonMark fenced-code tracking shared by both passes below, the
# same rules as the template's CHANGELOG guard: 0-3 spaces of indent, a run of
# 3+ backticks or tildes; closed only by the same character at least as long
# with nothing after it; an unclosed fence runs to end of input. infence()
# returns 1 for a line that opens, sits inside, or closes a fence.
fence_awk='
function fence(s,   i, n) {
	i = 1; while (i <= 3 && substr(s, i, 1) == " ") i++
	fch = substr(s, i, 1); if (fch != "~" && fch != "\140") return 0
	n = 0; while (substr(s, i + n, 1) == fch) n++
	info = substr(s, i + n); return n >= 3 ? n : 0
}
function infence(s,   n) {
	n = fence(s)
	if (open) { if (n >= open && fch == opench && info ~ /^[ \t]*$/) open = 0; return 1 }
	if (n && !(fch == "\140" && index(info, "\140"))) { open = n; opench = fch; return 1 }
	return 0
}'

# Locate, outside fenced code: the "## [Unreleased]" heading line, the line of
# the next "## " heading after it (the end of its body), and whether the
# version's own section heading already exists.
read -r ustart uend have < <(awk -v ver="$heading" "$fence_awk"'
	{ if (infence($0)) next }
	/^## \[Unreleased\][ \t]*$/ && !us { us = NR; next }
	us && !ue && /^## / { ue = NR }
	index($0, ver) == 1 { h = 1 }
	END { print us + 0, (ue ? ue : NR + 1), h + 0 }' CHANGELOG.md)
[ "$ustart" -gt 0 ] || die 'CHANGELOG.md has no "## [Unreleased]" heading to assemble under'

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
# The Unreleased body is the lines strictly between its heading and the next.
# With no line between them (the next heading directly follows), the body is
# empty: sed given a range whose end precedes its start would still print the
# start line — the next heading — so that case is produced explicitly.
if [ "$((uend - 1))" -ge "$((ustart + 1))" ]; then
	sed -n "$((ustart + 1)),$((uend - 1))p" CHANGELOG.md > "$tmp/unreleased"
else
	: > "$tmp/unreleased"
fi

frags=()
while IFS= read -r f; do frags+=("$f"); done < <(LC_ALL=C find changelog.d -maxdepth 1 -type f -name '*.md' 2>/dev/null | LC_ALL=C sort)

pending=0
[ "${#frags[@]}" -eq 0 ] || pending=1
! grep -q '[^[:space:]]' "$tmp/unreleased" || pending=1

if [ "$pending" -eq 0 ]; then
	[ "$have" -eq 1 ] || die "nothing to release: no changelog.d/ fragments, an empty Unreleased section, and no \"$heading\" section in CHANGELOG.md"
	echo "CHANGELOG.md already has $heading and nothing is pending; no assembly needed"
	exit 0
fi
[ "$have" -eq 0 ] || die "CHANGELOG.md already has a \"$heading\" section AND unreleased entries are pending (changelog.d/ fragments or Unreleased content). Fold them in by hand or remove that section; assembling would duplicate it"

# Group every "### " subsection by its exact heading text, in order of first
# appearance: the Unreleased content first, then each fragment in filename
# order. Lines before a source's first subsection heading form an untitled
# preamble. Chunks from different sources join with one blank line.
awk "$fence_awk"'
	function flush(   t) {
		t = chunk; sub(/^[ \t\n]+/, "", t); sub(/[ \t\n]+$/, "", t)
		if (t != "") body[cur] = (body[cur] == "" ? t : body[cur] "\n\n" t)
		chunk = ""
	}
	FNR == 1 { flush(); cur = ""; open = 0 }
	{
		if (!infence($0) && /^### /) {
			flush(); cur = $0; sub(/[ \t]+$/, "", cur)
			if (!(cur in seen)) { seen[cur] = 1; order[++k] = cur }
			next
		}
		chunk = chunk $0 "\n"
	}
	END {
		flush(); sep = ""
		if (body[""] != "") { printf "%s%s\n", sep, body[""]; sep = "\n" }
		for (i = 1; i <= k; i++) {
			if (body[order[i]] == "") continue
			printf "%s%s\n\n%s\n", sep, order[i], body[order[i]]; sep = "\n"
		}
	}' "$tmp/unreleased" "${frags[@]}" > "$tmp/section"

{
	sed -n "1,${ustart}p" CHANGELOG.md
	printf '\n%s - %s\n\n' "$heading" "$(date -u +%Y-%m-%d)"
	cat "$tmp/section"
	if [ "$uend" -le "$(($(wc -l < CHANGELOG.md)))" ]; then
		printf '\n'
		sed -n "${uend},\$p" CHANGELOG.md
	fi
} > "$tmp/CHANGELOG.md"
cat "$tmp/CHANGELOG.md" > CHANGELOG.md

"$CL_AGENT_GIT" "$CL_REPO" add CHANGELOG.md
if [ "${#frags[@]}" -gt 0 ]; then
	"$CL_AGENT_GIT" "$CL_REPO" rm -q -- "${frags[@]}"
fi
"$CL_AGENT_GIT" "$CL_REPO" commit -m "docs(changelog): assemble $plain for $ver"
echo "Assembled $heading from ${#frags[@]} fragment(s) and the Unreleased section"
