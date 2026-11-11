# syntax=docker/dockerfile:1.7
FROM golang:1.27.4-bookworm

ENV GOTOOLCHAIN=auto

WORKDIR /app

# The source is baked in rather than bind mounted. A containerised CI runner
# cannot bind mount its own workspace: the host daemon is asked for a path only
# the job container has ("mkdir /workspace: file exists").
COPY . .
# go.mod pins the internal first-party module github.com/jeremyhahn/go-enclave,
# published to the internal Gitea rather than github.com. Resolving it needs
# GOPRIVATE plus git's insteadOf rewrite carrying a read-only token
# (conventions/dependency-resolution.md §6), supplied as an OPTIONAL BuildKit
# secret carrying ONLY the tokenised GIT_CONFIG_KEY_0 value. It is mounted on
# tmpfs for this RUN alone and read via a quoted command substitution — never
# sourced as shell, so a credential containing shell metacharacters is assigned
# literally rather than executed. It is never a build arg or ENV (recoverable
# via docker history) and never written to a layer. The public parts of the
# rewrite are literals below; only the token-bearing key is secret. The mount
# is optional, so the same Dockerfile still builds against the public proxy
# when no secret is provided.
RUN --mount=type=secret,id=gomodauth \
    set -eu; \
    if [ -s /run/secrets/gomodauth ]; then \
        export GOPRIVATE='github.com/jeremyhahn/*'; \
        export GIT_CONFIG_COUNT=1; \
        export GIT_CONFIG_KEY_0="$(cat /run/secrets/gomodauth)"; \
        export GIT_CONFIG_VALUE_0='https://github.com/jeremyhahn/'; \
    fi; \
    go mod download

CMD ["go", "test", "-v", "-tags=integration,local", "./test/integration/replication/..."]
