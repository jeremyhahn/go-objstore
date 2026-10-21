### Fixed

- Concurrent CI jobs on one Docker host no longer tear each other's integration
  stacks down. Every integration target and `test-servers` runs under its own
  compose project (`COMPOSE_PROJECT`, `objstore-<GITHUB_RUN_ID>-<GITHUB_JOB>`
  plus a per-target suffix), the compose files no longer pin network names, and
  the Azurite and fake-gcs host port mappings are gone (the tests reach them by
  service name).
