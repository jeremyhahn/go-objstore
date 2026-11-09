# Shared Docker Compose project identity for every SDK Makefile in api/sdks.
#
# Compose derives its default project name from the directory ("go", "python",
# ...), so every checkout of every repository's SDK on a shared Docker daemon
# is one project: `docker compose down -v` from one run SIGTERMs (exit 143) the
# containers of another concurrent run. A shell PID is no fix: CI jobs in
# separate PID namespaces sharing one daemon collide on it, and `make clean`
# could not recompute it to remove what an interrupted run left behind.
#
# The name is a pure function of identities that are unique per daemon and
# stable for a checkout, so `make clean` recomputes the very project an
# interrupted `make test` used:
#   - the CI run, attempt and job (GITHUB_RUN_ID, GITHUB_RUN_ATTEMPT,
#     GITHUB_JOB) when set, else empty, and
#   - the absolute path of the SDK directory (distinct per checkout/worktree).
# Callers that already export COMPOSE_PROJECT_NAME keep their value.
#
# Include from an SDK Makefile:   include ../compose-project.mk
# The SDK's compose files publish no host ports: tests reach objstore-server over
# the compose network, so concurrent projects cannot collide on a host port.

#
# The hash is portable: sha256sum (GNU coreutils), else shasum -a 256 (macOS,
# Perl). With neither installed the make fails with an error: an empty hash
# must never yield a name, since worktrees would then share one project.
ifndef COMPOSE_PROJECT_NAME
compose_project_id := $(GITHUB_RUN_ID)/$(GITHUB_RUN_ATTEMPT)/$(GITHUB_JOB)/$(CURDIR)
compose_project_sq := '
compose_project_quoted := $(subst $(compose_project_sq),'\$(compose_project_sq)',$(compose_project_id))
compose_project_hash := $(shell { printf '%s' '$(compose_project_quoted)' | { sha256sum 2>/dev/null || shasum -a 256 2>/dev/null; }; } | cut -c1-12)
ifneq ($(words $(compose_project_hash)),1)
$(error compose-project.mk: cannot hash the checkout path: install sha256sum or shasum)
endif
COMPOSE_PROJECT_NAME := objstore-sdk-$(lastword $(subst /, ,$(CURDIR)))-$(compose_project_hash)
endif
export COMPOSE_PROJECT_NAME

# The compose secrets: read the gomodauth file from outside the checkout, and
# buildx bake's fs entitlement check refuses that build unless it is granted.
export BUILDX_BAKE_ENTITLEMENTS_FS := 0
