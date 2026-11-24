# Devcontainer stages: tools, base (Go toolchain, module cache, GOPROXY=off), toolchain (CI gates), capi (glibc, for make lib-test), integration (source baked in); build with `make docker-build`.

#
# The Go base images are pinned by tag and multi-arch index digest; the
# Makefile reads both from the two ARG lines below.
ARG GO_IMAGE=golang:1.27.4-alpine3.24@sha256:0a5f7fe796599ff487605987432e7619ae45beeb2fba4c2ce0b907879932aebf
# The capi stage's base: the same Go release as GO_IMAGE, on glibc. Raise the
# two tags, and their digests, together.
ARG GO_GLIBC_IMAGE=golang:1.27.4-trixie@sha256:b54b2639de8c1bfe538ec5ffc45f29b1932235ac5f6d63c059853646677112d9
ARG GO_BUILD_PROXY=https://proxy.golang.org,direct

# ---------------------------------------------------------------------------
FROM ${GO_IMAGE} AS tools
ARG GO_BUILD_PROXY

# GOTOOLCHAIN=local and CGO_ENABLED=0: static tools built by this toolchain.
ENV GOTOOLCHAIN=local \
    CGO_ENABLED=0

# Build-time only: install-tools.sh downloads with curl, unpacks with tar/xz
# and trusts what it fetched only through the SHA-256s in tool-pins.sh.
RUN apk add --no-cache curl tar xz

WORKDIR /build
COPY scripts/tool-pins.sh scripts/install-tools.sh scripts/
RUN GOPROXY="$GO_BUILD_PROXY" sh scripts/install-tools.sh /out

# ---------------------------------------------------------------------------
FROM ${GO_IMAGE} AS base
ARG GO_BUILD_PROXY

# GOTOOLCHAIN=local fails on a newer go.mod toolchain; CGO_ENABLED=1 for -race and the C library; module cache at /opt/go/mod.
ENV GOTOOLCHAIN=local \
    CGO_ENABLED=1 \
    GOFLAGS=-mod=readonly \
    GOMODCACHE=/opt/go/mod \
    GOCACHE=/gocache \
    GOTMPDIR=/tmp

# `apk upgrade` first, so the OS layer carries every published security fix
# and the image gate (`trivy image --ignore-unfixed`) scans the result. git:
# the authenticated fetch of the private module goes through it.
RUN set -eux; \
    apk upgrade --no-cache; \
    apk add --no-cache 'gcc~15.2' 'musl-dev~1.2.6' git; \
    addgroup -S -g 10001 dev; \
    adduser -S -D -H -u 10001 -G dev -h /home/dev -s /sbin/nologin dev; \
    install -d -o root -g root -m 0755 /opt/go/mod; \
    install -d -m 0755 -o 10001 -g 10001 /src; \
    install -d -o root -g root -m 0777 /gocache

# Dependencies first, every module's go.mod and go.sum at its own path; credentials stay in this RUN's shell.
WORKDIR /build
COPY go.mod go.sum ./
COPY backup/go.mod backup/go.sum backup/
COPY api/sdks/go/go.mod api/sdks/go/go.sum api/sdks/go/
COPY examples/encryption/go.mod examples/encryption/go.sum examples/encryption/
RUN --mount=type=secret,id=gomodauth \
    set -eu; \
    if [ -s /run/secrets/gomodauth ]; then \
        set -a; . /run/secrets/gomodauth; set +a; \
    fi; \
    for m in . backup api/sdks/go examples/encryption; do \
        (cd "$m" && GOPROXY="$GO_BUILD_PROXY" go mod download && go mod verify); \
    done; \
    chmod -R a+rX /opt/go/mod; \
    find /opt/go/mod/cache -type d -exec chmod a+w {} +

# Pre-warm the build cache: the race-instrumented standard library, plain and
# FIPS 140-3 (GOFIPS140=latest), then open it to every uid.
RUN set -eux; \
    go build -race std; \
    GOFIPS140=latest go build -race std; \
    chmod -R a+rwX /gocache

WORKDIR /src
RUN rm -rf /build

# From here on, and in both images, nothing fetches a module.
ENV GOPROXY=off

# ---------------------------------------------------------------------------
FROM base AS toolchain

ENV HOME=/tmp

# The GNU userland the CI runners have, so a Makefile recipe behaves here as it
# does there. safe.directory: the checkout copied in is owned by whoever ran
# it, and the releasability test reads its .git as any uid.
RUN apk add --no-cache \
        bash make git curl jq nodejs \
        coreutils grep sed gawk findutils diffutils tar xz; \
    git config --system --add safe.directory '*'

COPY --from=tools /out/ /usr/local/bin/

USER dev:dev
CMD ["go", "version"]

# ---------------------------------------------------------------------------
FROM ${GO_GLIBC_IMAGE} AS capi

# The official Debian image already carries gcc, libc6-dev, make and git. The
# module cache is base's, verified there; a module cache is the same on any
# libc, and nothing here fetches one.
ENV GOTOOLCHAIN=local \
    CGO_ENABLED=1 \
    GOFLAGS=-mod=readonly \
    GOMODCACHE=/opt/go/mod \
    GOCACHE=/gocache \
    GOTMPDIR=/tmp \
    GOPROXY=off \
    HOME=/tmp

RUN set -eux; \
    install -d -m 0755 -o 10001 -g 10001 /src; \
    install -d -o root -g root -m 0777 /gocache; \
    git config --system --add safe.directory '*'

COPY --from=base /opt/go/mod /opt/go/mod

USER 10001:10001
WORKDIR /src
CMD ["go", "version"]

# ---------------------------------------------------------------------------
FROM base AS integration

ENV HOME=/home/dev
RUN install -d -o dev -g dev -m 0700 /home/dev
# For a CI `container:` job's actions only.
RUN apk add --no-cache bash git tar nodejs
USER dev:dev
WORKDIR /src

# By name, not `COPY . .`: the context also carries the toolchain's inputs.
COPY --chown=root:root go.mod go.sum ./
COPY --chown=root:root api/proto/ api/proto/
COPY --chown=root:root cmd/ cmd/
COPY --chown=root:root pkg/ pkg/
COPY --chown=root:root internal/ internal/
COPY --chown=root:root test/integration/ test/integration/

# Fail the build if the tree cannot be compiled.
RUN go vet -tags "local awss3 gcpstorage azureblob glacier azurearchive integration" ./test/integration/...

CMD ["go", "version"]
