### Changed

- `make release-alpha` is the company release-alpha template v2.3.9, vendored
  verbatim at `mk/release-alpha.mk`, replacing the v1 recipe (issue #32). The
  version is now an explicit argument (`make release-alpha VERSION=vX.Y.Z-alpha`)
  rather than a patch-only auto-increment; every git write signs through
  `agent-git.sh` as `agent-go-objstore`; the tag is annotated; only the release
  commit and that one tag are pushed, to `origin`; and the recipe fails closed
  on a wrong branch, a dirty tree, an existing tag, an unreachable remote or a
  missing CHANGELOG section. The Makefile's build version variable is renamed
  `BUILD_VERSION` so it no longer shadows the `VERSION=` argument. The
  repository's release wiring lives in `mk/release.mk`.
- CHANGELOG entries are now `changelog.d/<issue>-<slug>.md` fragments
  (release-versioning.md §5). `make release-alpha` first runs
  `changelog-assemble`, which folds the fragments and any `## [Unreleased]`
  content into the version's section, grouped by subsection, deletes the
  consumed fragments and commits that, signed, before the tag is created.

### Build

- `make test-release-alpha` runs the template's own regression harness and
  `mk/testdata/release-path-tests.sh`, which drives the repository's release
  path against a throwaway remote: explicit-version refusal, wrapper arguments,
  an annotated single tag, a failed commit leaving no tag and no push, the
  assembled CHANGELOG in the tagged tree, and no command injection through any
  release input.
