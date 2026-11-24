#!/bin/sh
# deps-check.sh [module-dir ...]: fail when a required module is behind its latest release without a "// held: <reason>" comment.
set -eu

prog=${0##*/}
die() {
  echo "$prog: $*" >&2
  exit 1
}

go=${GOCMD:-go}
GOWORK=off
export GOWORK
case ${GOPROXY:-} in off) GOPROXY=https://proxy.golang.org,direct; export GOPROXY ;; esac

# held_reason GOMOD MODULE prints "held <reason>", "listed", or nothing when not required.
held_reason() {
  _state=
  while IFS= read -r _line || [ -n "$_line" ]; do
    _rest=${_line#"${_line%%[![:space:]]*}"}
    case $_rest in
      require[[:space:]]* | replace[[:space:]]*)
        _rest=${_rest#*[[:space:]]}
        _rest=${_rest#"${_rest%%[![:space:]]*}"}
        ;;
    esac
    case $_rest in "$2"[[:space:]]*) ;; *) continue ;; esac
    case $_line in
      *'// held:'*)
        _why=${_line#*// held:}
        _why=${_why#"${_why%%[![:space:]]*}"}
        if [ -n "$_why" ]; then
          echo "held $_why"
          return 0
        fi
        ;;
    esac
    _state=listed
  done <"$1"
  [ -z "$_state" ] || echo "$_state"
}

[ $# -gt 0 ] || set -- . backup api/sdks/go examples/encryption
bad=0
for dir in "$@"; do
  gomod=$dir/go.mod
  [ -f "$gomod" ] || die "$gomod: no such file"
  behind=$(cd "$dir" && "$go" list -m -u \
    -f '{{if and (not .Main) .Update}}{{.Path}} {{.Version}} {{.Update.Version}}{{end}}' all) ||
    die "go list -m -u all failed in $dir"
  while read -r mod cur latest; do
    [ -n "$mod" ] || continue
    state=$(held_reason "$gomod" "$mod")
    case $state in
      '') ;;
      held*) echo "$prog: $gomod: $mod $cur is held below $latest: ${state#held }" ;;
      *)
        echo "$prog: $gomod: $mod $cur is behind $latest, with no '// held: <reason>' comment" >&2
        bad=$((bad + 1))
        ;;
    esac
  done <<EOF
$behind
EOF
done
[ "$bad" -eq 0 ] || die "$bad module(s) behind their latest release without a documented reason"
echo "$prog: every required module is at its latest release or held with a reason"
