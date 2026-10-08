# syntax=docker/dockerfile:1.7
# MinIO server for the S3 and MinIO integration tests, built from source.
#
# The published images (quay.io/minio/minio, minio/minio) refuse anonymous pulls
# for every tag, so the tests build the server from the AGPL source instead
# (owner ruling 2026-10-08). This is the one MinIO Dockerfile; the root
# docker-compose.yml, test/integration/s3 and test/integration/minio compose
# files all build it. The build needs network access to github.com and the Go
# module proxy.
#
# Build context: this directory (nothing from the repository is copied in).

# The one place the MinIO release is pinned. Bump it here and nowhere else.
ARG MINIO_TAG=RELEASE.2025-10-15T17-29-55Z

FROM golang:1.27.1-alpine AS builder

ARG MINIO_TAG

RUN apk add --no-cache git

# Shallow clone of the release tag, then a static build with the ldflags MinIO's
# own release build sets (cmd.Version is the release time, ReleaseTag the tag).
RUN set -eu; \
    git clone --depth 1 --branch "${MINIO_TAG}" https://github.com/minio/minio.git /src; \
    cd /src; \
    VERSION="$(echo "${MINIO_TAG#RELEASE.}" | sed -E 's/T([0-9]{2})-([0-9]{2})-([0-9]{2})Z/T\1:\2:\3Z/')"; \
    COMMIT="$(git rev-parse HEAD)"; \
    CGO_ENABLED=0 go build -trimpath -o /out/minio \
      -ldflags "-s -w \
        -X github.com/minio/minio/cmd.Version=${VERSION} \
        -X github.com/minio/minio/cmd.ReleaseTag=${MINIO_TAG} \
        -X github.com/minio/minio/cmd.CommitID=${COMMIT} \
        -X github.com/minio/minio/cmd.ShortCommitID=$(echo "${COMMIT}" | cut -c1-12)" \
      .

FROM alpine:3.22

# curl serves the compose healthcheck (/minio/health/live).
RUN apk add --no-cache ca-certificates curl

COPY --from=builder /out/minio /usr/bin/minio

EXPOSE 9000 9001
VOLUME ["/data"]

# Same contract as the published image: the compose `command:` is the minio
# subcommand and its arguments (`server /data --console-address ":9001"`).
ENTRYPOINT ["/usr/bin/minio"]
CMD ["server", "/data"]
