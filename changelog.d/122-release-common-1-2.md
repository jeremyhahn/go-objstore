### Changed

- `mk/template/` is the handbook's release templates byte for byte, at
  `release-common.mk` 1.2.0. The lockstep tags, the replace-free release commit,
  the restore commit and the post-push consumer check are set through the
  template's `RELEASE_REPLACES`, `RELEASE_SYNC_TARGETS` and
  `RELEASE_CONSUMER_MODULES` in `mk/release.mk`; the repo-local
  `consumer-check` and `release-consumer-check` targets are removed.
