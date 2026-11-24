#!/usr/bin/env bash
# changelog-assemble.sh folds changelog.d/ fragments and Unreleased into the version's CHANGELOG section and commits it signed, before tagging; inputs are CL_VERSION, CL_AGENT_GIT, CL_REPO, CL_REMOTE and CL_MODULES.
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
tags=("$ver")
seen=' '
for m in ${CL_MODULES:-}; do
	printf '%s' "$m" | grep -Eq '^[A-Za-z0-9][A-Za-z0-9._-]*(/[A-Za-z0-9][A-Za-z0-9._-]*)*$' \
		|| die "LOCKSTEP_MODULES entry is not a plain repo-relative directory: $m"
	case "$seen" in *" $m "*) die "LOCKSTEP_MODULES lists $m twice" ;; esac
	seen="$seen$m "
	git ls-files --error-unmatch -- "$m/go.mod" >/dev/null 2>&1 \
		|| die "LOCKSTEP_MODULES entry $m has no tracked go.mod — a lockstep tag must name a real nested module"
	tags+=("$m/$ver")
done
for t in "${tags[@]}"; do
	! git rev-parse -q --verify "refs/tags/$t" >/dev/null || die "tag $t already exists locally"
	rc=0; git ls-remote --exit-code --tags "$remote" "refs/tags/$t" >/dev/null 2>&1 || rc=$?
	case "$rc" in
		0) die "tag $t already exists on $remote" ;;
		2) : ;;
		*) die "could not reach $remote to check whether tag $t exists. Refusing to continue: an unreachable remote is not an absent tag" ;;
	esac
done
[ -z "$(git status --porcelain)" ] || die 'working tree not clean'
[ -f CHANGELOG.md ] || die 'no CHANGELOG.md'

plain="${ver#v}"
heading="## [$plain]"

# fence_awk tracks CommonMark fenced code; infence() returns 1 for a line that opens, sits in or closes a fence.
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
# The Unreleased body is the lines strictly between its heading and the next; adjacent headings mean an empty body.
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

# Group "### " subsections by heading in first-appearance order, Unreleased first, then fragments by filename.
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
