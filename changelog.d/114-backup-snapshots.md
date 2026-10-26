### Added

- Source snapshots for crash-consistent backups, issue #114. The service's
  `source_snapshot` setting and the `backup` command's `--snapshot` flags
  snapshot each source volume once per run, read the sources inside the
  snapshots, and remove them after the run, on failure and on cancellation
  too. The methods are `lvm`, `btrfs`, `zfs`, `apfs` (macOS), `vss` (Windows
  Server) and `auto`, which picks one by file system; the default `none`
  reads the live sources. The tools run by absolute path with an argument
  vector, never through a shell. Snapshot names carry a fixed prefix and a
  random suffix; leftovers with the prefix are removed at service start and
  before each run, and nothing without it is touched. The failure policy is
  `fail` (the default) or `fallback`, which logs a warning and backs up the
  live sources. Snapshots record the original source paths, so a restore is
  unchanged. Package `backup/pkg/srcsnap` holds the seam, and
  `backup.Options.ReadFrom` points a backup at the snapshot.
