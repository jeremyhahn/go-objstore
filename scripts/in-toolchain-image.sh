#!/usr/bin/env bash
# in-toolchain-image.sh <image> <command...>: run the command in the toolchain image on a copy of the checkout, hardened, with cache volumes and the job's CPU bound.
set -euo pipefail

if [ "$#" -lt 2 ]; then
  echo "usage: $0 <image> <command...>" >&2
  exit 2
fi
image=$1
shift
network=${IN_IMAGE_NETWORK:-none}
case $network in
  none | bridge) ;;
  *) echo "IN_IMAGE_NETWORK must be none or bridge: $network" >&2; exit 2 ;;
esac

uid=${IN_IMAGE_UID:-10001}
case $uid in
  "" | 0 | *[!0-9]*) echo "IN_IMAGE_UID must be a non-root numeric uid: $uid" >&2; exit 2 ;;
esac
prefix=${CI_STACK_PREFIX:-$(basename "$PWD")-ci-}
name="${prefix}tc-$$-${RANDOM}"
cache=${CI_CACHE:-on}
case $cache in
  on | off) ;;
  *) echo "CI_CACHE must be on or off: $cache" >&2; exit 2 ;;
esac
class=${CI_CACHE_CLASS:-pr}
case $class in
  pr | main) ;;
  *) echo "CI_CACHE_CLASS must be pr or main: $class" >&2; exit 2 ;;
esac
cpus=${IN_IMAGE_CPUS:-}
if [ -z "$cpus" ]; then
  quota=max period=100000
  if [ -r /sys/fs/cgroup/cpu.max ]; then read -r quota period < /sys/fs/cgroup/cpu.max; fi
  case $quota in
    max | "" | *[!0-9]*) cpus=$(nproc) ;;
    *) cpus=$(((quota + period - 1) / period)) ;;
  esac
fi
case $cpus in
  "" | 0 | *[!0-9]*) echo "IN_IMAGE_CPUS must be a positive integer: $cpus" >&2; exit 2 ;;
esac
args=(
  --name "$name"
  --cpus "$cpus"
  --env GOMAXPROCS="$cpus"
  --env GOFLAGS="-p=$cpus"
  --init
  --cap-drop ALL
  --security-opt no-new-privileges:true
  --pids-limit 4096
  --ulimit memlock=67108864:67108864
  --ulimit core=1048576:1048576
  --ulimit nofile=65536:65536
  --tmpfs "/tmp:rw,exec,nosuid,nodev,size=3g,mode=1777"
  --network "$network"
  --user "$uid:$uid"
  --workdir /src
  --env HOME=/tmp
  --env CI="${CI:-}"
)
modcache=/tmp/gomodcache
if [ "$cache" = on ]; then
  vols="${prefix}cache-${class}"
  docker run --rm --network none --cap-drop ALL --cap-add CHOWN \
    --security-opt no-new-privileges:true --user 0:0 \
    -v "$vols-gobuild:/c/gobuild" -v "$vols-golangci:/c/golangci" -v "$vols-gomod:/c/gomod" \
    "$image" sh -c "for d in /c/*; do [ \"\$(stat -c %u:%g \"\$d\")\" = $uid:$uid ] || chown $uid:$uid \"\$d\"; done" >/dev/null
  args+=(
    -v "$vols-gobuild:/cache/gobuild" --env GOCACHE=/cache/gobuild
    -v "$vols-golangci:/cache/golangci" --env GOLANGCI_LINT_CACHE=/cache/golangci
  )
  if [ "$network" = bridge ]; then
    args+=(-v "$vols-gomod:/cache/gomod")
    modcache=/cache/gomod
  fi
fi
if [ "$network" = bridge ]; then
  # The baked module cache is read-only to the run's user and holds only the
  # root module's build list; the nested modules deps-check walks fetch more.
  args+=(--env "GOPROXY=https://proxy.golang.org,direct" --env "GOMODCACHE=$modcache")
  for v in GOPRIVATE GIT_CONFIG_COUNT GIT_CONFIG_KEY_0 GIT_CONFIG_VALUE_0; do
    if [ -n "${!v:-}" ]; then args+=(--env "$v"); fi
  done
fi
# LABSTRINGS_PATTERNS is copied in mode 0400 for no-network stages only.
patterns=${LABSTRINGS_PATTERNS:-}
if [ -n "$patterns" ]; then
  if [ ! -f "$patterns" ] || [ -L "$patterns" ]; then
    echo "LABSTRINGS_PATTERNS must name a regular file" >&2
    exit 2
  fi
  if [ "$network" = none ]; then
    args+=(--env LABSTRINGS_PATTERNS=/labstrings-patterns)
  else
    patterns=
  fi
fi

cleanup() { docker rm -f -v "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT

docker create "${args[@]}" "$image" sh -c "$*" >/dev/null
tar -C "$PWD" --owner="$uid" --group="$uid" --numeric-owner -cf - . |
  docker cp -a - "$name:/src"
if [ -n "$patterns" ]; then
  tar -C "$(dirname "$patterns")" --owner="$uid" --group="$uid" --numeric-owner --mode=0400 \
    --transform 's|.*|labstrings-patterns|' -cf - "$(basename "$patterns")" |
    docker cp -a - "$name:/"
fi
docker start -a "$name"
