### Changed

- Published files no longer name the private development fleet or its owner
  (issue #51). A test-only gate, `internal/labstrings`, enforces it under
  `make test`; `.gitattributes` and `.dockerignore` keep the agent development
  tooling out of archives and image build contexts. See
  `docs/release/public-releasability.md`.
