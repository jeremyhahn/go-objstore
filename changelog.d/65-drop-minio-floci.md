### Removed

- **BREAKING:** the `minio` storage provider is gone, issue #65: `pkg/minio`,
  the `minio` build tag, `WITH_MINIO`, the `minio` factory registration, the
  `minio` value of the CLI `--backend` flag and of the replication backend
  arguments, `integration-test-minio` and `bench-minio`, and
  `test/integration/minio`. MinIO is no longer open source, so go-objstore no
  longer supports it. An S3-compatible endpoint is reached through the `s3`
  provider's `endpoint` option. `cli.ErrBackendURLRequired` is removed with its
  only user.

### Changed

- The S3 integration suite and the root integration compose file run against
  Floci (`floci/floci` 2.2.0, pinned by digest; S3 on port 4566, health check
  the image's `/usr/local/bin/healthcheck.sh`, Docker-backed services disabled)
  instead of MinIO.
