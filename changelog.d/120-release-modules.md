### Changed

- `make release-alpha` now cuts `vX.Y.Z`, `backup/vX.Y.Z` and `api/sdks/go/vX.Y.Z`
  in one signed, atomic push. The tagged commit requires the root and Go SDK
  modules at the new version with no in-repo `replace`; a follow-up commit
  restores the replaces on `main` so development keeps building. After the push,
  the recipe resolves, builds and installs each of the three modules at the new
  tag from a scratch module.
- The release workflow ships every `cmd/` binary and `objstore-backup` in each
  platform archive, each with its SHA-256 checksum.
