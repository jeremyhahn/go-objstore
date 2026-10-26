# go-objstore devcontainer: one Dockerfile, the shipped stages built FROM base.
#
#   tools        build-only: the pinned tools, built or downloaded into /out
#   base         the Go toolchain, the C compiler -race and the C API need, a
#                non-root user and the verified module cache of every module
#                in the tree; GOPROXY=off
#   toolchain    base + every tool the Makefile calls. CI runs `make lint`,
#                `make build`, `make coverage`, `make security`, `make
#                deps-check`, `make backup-lint` and `make backup-test` here
#                (scripts/in-toolchain-image.sh). The checkout is COPIED in,
#                never baked; `make toolchain-image` prints its
#                content-addressed tag.
#   capi         the same Go release on glibc (Debian) with base's module
#                cache: CI runs `make lib-test` here. A Go c-shared library
#                cannot load on musl: the Go runtime reads argc and argv from
#                the arguments of its ELF constructor, which glibc passes and
#                musl does not, so on Alpine it segfaults before main.
#                `make capi-image` prints its tag.
#   integration  base + the Go source, BAKED IN: the image CI names with
#                `make it-image` and scans with `trivy image`. The integration
#                suites under test/integration build their own images through
#                their compose files.
#
# Build with `make docker-build`, which passes GO_IMAGE, GO_BUILD_PROXY and the
# gomodauth secret.
#
# THE GO TOOLCHAIN is the one every go.mod declares (`go 1.27.3`), from the
# official Alpine image. Raise the tag in the same change that moves go.mod; to
# pin by digest, `docker buildx imagetools inspect golang:<ver>-alpine<branch>`
# and append `@sha256:<index digest>` to the tag.
#
# MODULE AUTHENTICATION: github.com/jeremyhahn/go-enclave is fetched with the
# forge credentials, which reach the build ONLY as the BuildKit secret
# `gomodauth` (`make stage-gomodauth`): an env file of GOPRIVATE,
# GIT_CONFIG_COUNT, GIT_CONFIG_KEY_0 and GIT_CONFIG_VALUE_0, `KEY=value` or
# `export KEY=value` lines. A secret mount is never written to a layer, and
# the RUN that reads it prints nothing of it. An empty or absent secret means
# the modules resolve without credentials.
#
# FETCHING happens only at build time and only through GO_BUILD_PROXY. The
# images' ENV is GOPROXY=off, so a gate that reaches for a module fails instead
# of fetching it. CI runs `make security` and `make deps-check` with the
# network on, and they set their own proxy.
#
# There is deliberately no `# syntax=` directive: it would make every build
# pull a frontend image by a floating tag. BuildKit's built-in frontend
# supports everything used here, Dockerfile.dockerignore included.
#
# Every file a COPY in the tools, base or toolchain stage reads is in the
# Makefile's TOOLCHAIN_INPUTS, so a change to one is a new toolchain tag.

ARG GO_IMAGE=golang:1.27.3-alpine3.24
# The capi stage's base: the same Go release as GO_IMAGE, on glibc. Raise the
# two tags together.
ARG GO_GLIBC_IMAGE=golang:1.27.3-trixie
ARG GO_BUILD_PROXY=https://proxy.golang.org,direct

# ---------------------------------------------------------------------------
FROM ${GO_IMAGE} AS tools
ARG GO_BUILD_PROXY

# GOTOOLCHAIN=local: the Go tools are built by THIS toolchain. CGO_ENABLED=0:
# static binaries.
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

# GOTOOLCHAIN=local makes a go.mod asking for a newer toolchain FAIL rather
# than download it. CGO_ENABLED=1 is for -race and for the C shared library
# (`make lib`). The caches sit at fixed paths outside any home; the module
# cache is /opt/go/mod, the path ci.yml's `trivy image` step skips.
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

# Dependencies first: a source-only change reuses this layer. Every module's
# go.mod and go.sum, at its own path, because the nested modules replace the
# root module by relative path. The credentials are exported from the secret
# in this RUN's shell only; nothing echoes them and nothing writes them down.
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

# Fail the BUILD, not a run, if the tree that arrived cannot be compiled.
RUN go vet -tags "local awss3 gcpstorage azureblob glacier azurearchive integration" ./test/integration/...

CMD ["go", "version"]
