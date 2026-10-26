### Added

- `objstore-backup service --config <file>` runs the backup agent as a
  scheduled service on Linux, macOS and Windows, issue #114. The JSON
  configuration file names the backup identity's connection, the repository,
  the passphrase file, the sources, the host, the schedule (`every` a fixed
  interval or `daily` at `HH:MM`, with optional `jitter`) and an optional
  retention policy run as the prune identity over its own connection. Unknown
  settings, an inline passphrase or token, and a file group or others can
  write are refused. Each run reads the passphrase from its file and closes
  the repository after it, so the data key is not held between runs. Runs
  are logged with `log/slog`.
- `backup/packaging` holds a hardened systemd service, a launchd daemon plist
  and an example configuration file. On Windows the binary runs under the
  service control manager and is installed with `sc.exe`.

### Changed

- On Windows, objstore-backup checks a passphrase, token or configuration
  file against its ACL rather than its permission bits, which there reflect
  only the read-only attribute.
