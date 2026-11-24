# syntax=docker/dockerfile:1.7
FROM golang:1.27.4-bookworm

RUN apt-get update && apt-get install -y ca-certificates wget netcat-openbsd && rm -rf /var/lib/apt/lists/*

ENV GOTOOLCHAIN=auto

WORKDIR /app

# The source is baked in rather than bind mounted. A containerised CI runner
# cannot bind mount its own workspace: the host daemon is asked for a path only
# the job container has ("mkdir /workspace: file exists").
COPY . .
# Resolve the private go-enclave module through an optional BuildKit secret holding only the token key, read on tmpfs and never kept in a layer.
RUN --mount=type=secret,id=gomodauth \
    set -eu; \
    if [ -s /run/secrets/gomodauth ]; then \
        export GOPRIVATE='github.com/jeremyhahn/*'; \
        export GIT_CONFIG_COUNT=1; \
        export GIT_CONFIG_KEY_0="$(cat /run/secrets/gomodauth)"; \
        export GIT_CONFIG_VALUE_0='https://github.com/jeremyhahn/'; \
    fi; \
    go mod download

# Wait for fake-gcs to be ready before running tests
CMD ["sh", "-c", "echo 'Waiting for fake-gcs to be ready...'; for i in 1 2 3 4 5 6 7 8 9 10; do wget -q --spider http://fake-gcs:4443/storage/v1/b && break || sleep 2; done; echo 'fake-gcs is ready, running tests...'; go test -v -tags=integration,gcpstorage ./test/integration/gcs/..."]
