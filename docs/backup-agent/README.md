# backup-agent

> **Status: SHIPPED** · Scope: go-objstore backup-agent · Last verified: 2026-10-26
> The owner ruled the direction (CA m953, rulings 112/113) and the SPEC row is `shipped`, with all six slices in. The agent is built in its own module, `backup/`; restore and verify (`backup/pkg/restore`) and prune (`backup/pkg/prune`) are libraries there, and the `objstore-backup` command drives them (see Usage). `objstore-backup service` runs them on a schedule as a Linux, macOS or Windows service (see Service), and both can read the sources from a snapshot of each volume for crash consistency (see Source snapshots). Keys come from the standalone passphrase provider or, through the key seam, from the go-xkms provider that go-xkms implements in its own repository.

## Requirement

Owner order (Company Agent m910 (3), 2026-10-21): a backup agent as a go-objstore deliverable, deployable to any system, that backs the system up to a bucket, compressed, encrypted and incremental.

## Decision

Owner rulings (CA m953, items 112/113, 2026-10-23; ruling 175, 2026-10-25, which supersedes 112/113 on where the agent is built and where the seam lives):

- The backup agent is a go-objstore deliverable: one binary, `objstore-backup`, built from its own Go module inside this repository, `backup/` (module `github.com/jeremyhahn/go-objstore/backup`, its own go.mod), on the xkey pattern: a product-surface module in a subdirectory that consumes the project's SDK. It is not a separate repository.
- The `backup/` module consumes the Go client SDK module (`api/sdks/go`) and talks to an objstore server through it. Its only first-party dependencies are that SDK module and go-enclave; it never imports go-xkms.
- Keys come from go-xkms and reach the agent through the key seam below, whose interface is defined in `backup/`. go-xkms implements it downward, in its own repository. go-objstore's root module keeps go-enclave as its only first-party dependency.
- The agent is a client only: it adds no endpoint and no change to `common.Storage`. The server gains only configurable gRPC message limits (`grpc.max_recv_msg_size`, `grpc.max_send_msg_size`).
- No code until the SPEC row is approved.

The agent stores a snapshot repository of immutable packs, an index and snapshot manifests as objects on the server it is pointed at. Because it uses only the SDK's object operations, it works on every backend the server fronts.

## Architecture

- **Chunking.** FastCDC splits each file into content-defined chunks, so an edit inside a large file re-uploads only the changed chunks.
- **Chunk identity.** Each chunk ID is HMAC-SHA-256 of the chunk under a repository secret. The agent uploads only chunks the index does not already hold.
- **Packing.** Chunks are compressed with the algorithm the sealed format records (`none` today; zstd or gzip waits on the open item below), then encrypted, then grouped into immutable pack objects written once.
- **Encryption.** Each pack, index, snapshot and config object is sealed with AES-256-GCM under a subkey derived with HKDF-SHA-256 from the repository data key.
- **Snapshot format.** Immutable packs; an index object per backup run that stores new chunks, mapping chunk IDs to packs, with all of them merged on load; and one snapshot manifest per backup run (tree, file metadata, chunk lists), written last. Objects are written once under `config`, `keys/`, `keyring/` (key-list generations, see Key management), `packs/<id[:2]>/`, `index/`, `snapshots/` and, for prune, `marks/`, each named by a keyed ID of its content; time and host are only inside the sealed manifest.
- **Key seam.** The data key is wrapped by a KEK that a key provider supplies. The interface is `keyseam.Provider` in the `backup/` module (`backup/pkg/keyseam`): `WrapKey(ctx, dataKey)` and `UnwrapKey(ctx, wrapped)`, with typed errors that carry no key material. The provider is a go-xkms client that implements the interface downward, in go-xkms; neither go-objstore module imports go-xkms. The unwrapped data key lives in go-enclave locked memory and is destroyed when the repository closes; only its wrapped form is persisted. Standalone mode, with no KMS, derives the KEK from a passphrase with PBKDF2-HMAC-SHA-256 (a random 16-byte salt stored beside the wrapped key, at least 600000 iterations) and holds it in go-enclave locked memory.
- **Restore.** Restore reads a snapshot manifest, fetches and decrypts the chunks it lists, rebuilds files and metadata, and verifies every chunk ID and file digest end to end. `verify` runs the same check on a repository without writing files.
- **Prune.** Retention prunes snapshots from a separate, privileged identity. The backup identity needs no delete right, so the repository works on a WORM bucket. In code, removal is a `store.PruneStore` that only `backup/pkg/prune` holds; a repository never hands it on. A policy keeps the last N snapshots, those within a duration, and the newest of each of the last N days, ISO weeks, months and years, by the time sealed in each manifest; it keeps the union. The policy applies to each group of snapshots with the same host and source directory on its own, so with several sources (or hosts) each keeps its own newest snapshots; the report lists each group's kept and forgotten snapshots (`groups` in `--json` output). A policy that would keep no snapshot of a group, such as `--keep-within` alone for a source whose backups stopped, is refused, as is one that keeps nothing. Prune takes no lock, which the backup identity could not remove; it marks and sweeps. Each run writes a sealed mark under `marks/` naming the packs no kept snapshot uses, each with the time a run first marked it. A backup reads every mark before it loads the index, and neither dedups against a marked pack nor writes one under a marked name: it stores those chunks again under another name. A run removes a pack only if it is still unused and the newest earlier mark has named it for a grace period (24 hours by default) that must exceed the longest backup, so a backup that started before the mark has written its snapshot by then and its chunks keep their packs. One prune runs at a time. A run writes the new index first, then removes forgotten snapshots, superseded indexes, unused packs and old marks, so an interrupted run leaves every kept snapshot restorable and a rerun completes it. Partly used packs are kept and their unused bytes reported; repacking is not done. A dry run reports what would go and changes nothing, and `verify` reports packs that no index names.
- **Service.** The agent runs on Linux, macOS and Windows as a scheduled service, with optional source snapshots (VSS, LVM, btrfs, ZFS, APFS) for crash consistency; see Source snapshots.

## Usage

`objstore-backup` (`backup/cmd/objstore-backup`) is a standalone command over the libraries, using the passphrase key provider:

```
objstore-backup init      --address <host:port> --repo <prefix> --passphrase-file <file>
objstore-backup backup    [flags] [--host <name>] [--snapshot <method>] <path>...
objstore-backup snapshots [flags] [--json]
objstore-backup restore   [flags] [--overwrite] <snapshot> <target>
objstore-backup verify    [flags] [--json] [<snapshot>...]
objstore-backup prune     --prune-address <host:port> --repo <prefix> --passphrase-file <file> \
                          [--keep-last n] [--keep-within 30d] [--keep-daily n] [--keep-weekly n] \
                          [--keep-monthly n] [--keep-yearly n] [--grace 24h] [--dry-run] [--json]
objstore-backup service   --config <file> [--check]
objstore-backup key       list|add|remove <slot>|passwd [flags] [--new-passphrase-file <file> | --new-passphrase-stdin]
```

- **Connection.** The server is reached through the Go client SDK: `--protocol` (rest, grpc, quic, mcp, unix), `--address`, `--tls`, `--ca-file`, `--cert-file`, `--key-file`, `--token-file`, `--tenant`, `--timeout`. `--repo` is the prefix the repository lives under.
- **gRPC message limit.** Over gRPC each pack is uploaded in one message, so the server's `grpc.max_recv_msg_size` must be at or above the repository's pack size plus a little framing; the server default of 32 MiB fits the default 16 MiB pack.
- **Prune identity.** `prune` takes none of those: it connects with its own `--prune-*` set, which must name a delete-capable identity. The backup identity needs no delete right and should have none.
- **Passphrase.** From `--passphrase-file`, refused unless it is a regular file with no group or other permission bits, or from standard input with `--passphrase-stdin`; never from an argument or the environment. The read buffer is cleared once the passphrase is in go-enclave locked memory. A bearer token file follows the same permission rule.
- **Snapshots** are named by ID or by a prefix that names one snapshot only.
- **Exit status.** 0 success, 1 failure, 2 usage error, 3 `verify` found problems.

## Key management

A repository holds one data key, which every pack, index, snapshot and config object is sealed under, and one or more key slots: each slot is that data key wrapped under one passphrase, with its own slot ID and its own PBKDF2 salt. Opening the repository tries each slot until one unwraps, so any slot's passphrase opens it. Snapshots are never re-encrypted; a key change rewrites only the wrap of the data key.

```
objstore-backup key list   [flags]
objstore-backup key add    [flags] --new-passphrase-file <file> | --new-passphrase-stdin
objstore-backup key remove [flags] <slot>
objstore-backup key passwd [flags] --new-passphrase-file <file> | --new-passphrase-stdin
```

- **`key list`** lists the slots; `*` marks the one the current passphrase opened.
- **`key add`** adds a slot for the new passphrase. Both passphrases then open the repository.
- **`key remove <slot>`** removes a slot, named by its ID or by a prefix that names one slot only. The last slot is refused, so a repository always has a passphrase that opens it.
- **`key passwd`** wraps the data key under the new passphrase and removes the slot the current passphrase opened. Every existing snapshot restores with the new passphrase; the old one is refused.
- **Passphrases.** The current passphrase comes from `--passphrase-file` or `--passphrase-stdin`, and the new one from `--new-passphrase-file` or `--new-passphrase-stdin`, under the same rules (see Usage); only one of them can come from standard input. Neither is ever taken from an argument or the environment.
- **`--key-provider`** (every command; default `passphrase`) chooses the `keyseam.Provider` by name through the registry in `backup/pkg/keyseam` (`NewProvider`, `Register`). Only `passphrase` is built in; an unknown name is a usage error (`keyseam.ErrUnknownProvider`). `Register` is the seam a key-management provider such as go-xkms's plugs into later; the `backup/` module does not import one.

### Repository format

Format 2 adds key slots. The packs, indexes and snapshots are written once and never replaced, so a key change does not rewrite `config`: it writes a new, sealed, immutable generation of the key list under `keyring/` (`keyring/<n>`, 16 hex digits), naming the slots (`keys/` objects) that open the repository. Opening uses the latest generation and refuses a key whose slot it no longer lists.

A format 1 repository, written before key slots, opens as before with its single key, and is upgraded to format 2 by its first key change (`key add`, `key remove` or `key passwd`); until then nothing in it changes. There is no separate upgrade step.

Two consequences of the write-once store:

- **A removed slot's key object stays in the store.** `key remove` and `key passwd` drop the slot from the new key list, and the current binary refuses it, but its object under `keys/` is not deleted (the backup identity has no delete right). Anyone holding that object and the old passphrase can still unwrap the data key. If the old passphrase is compromised, delete the object with a delete-capable identity, or treat the data key as exposed and start a new repository.
- **An older binary ignores `keyring/`.** A binary from before format 2 reads only `config` and `keys/`, so on an upgraded repository it still accepts the passphrase of the original key even after its slot was removed. Upgrade every host that opens the repository before relying on `key remove` or `key passwd`.

## Service

`objstore-backup service --config <file>` runs the agent as a long-lived process that backs its sources up on a schedule, and prunes after each backup if the configuration has a `prune` section. `--check` loads and checks the file and exits. Each run reads the passphrase (and any token) from its file, opens the repository, backs each source up to its own snapshot, prunes, and closes the repository, so the data key is destroyed until the next run; nothing secret is held between runs. A failed run is logged and the service waits for the next. SIGTERM, SIGINT or a Windows service stop request cancels the current run, abandoning any pack upload (a rerun reuses what reached the server), closes the repository and exits 0.

### Configuration file

One JSON object (see `backup/packaging/config.example.json`). The file must be a regular file that group and others cannot write (on Windows: whose ACL grants no write right to Everyone, Authenticated Users, Users, Guests or Anonymous). Unknown settings, a missing required setting, and a passphrase or token given inline are refused, naming the setting and never its value.

| Setting | Required | Meaning |
|---|---|---|
| `connection` | yes | The backup identity's server connection: `protocol` (rest, grpc, quic, mcp, unix; default grpc), `address` (required), `tls`, `ca_file`, `cert_file`, `key_file`, `token_file`, `tenant`, `timeout`. These are the CLI's connection flags. |
| `repo` | no | The repository prefix, as `--repo`. |
| `passphrase_file` | yes | The passphrase file, under the CLI's rule: a regular file with no group or other permission (on Windows: no ACL entry for those groups). `passphrase` is refused. |
| `sources` | yes | The directories to back up, absolute paths; each run writes one snapshot per source. The backup path has no excludes yet, so there is no exclude setting. |
| `host` | no | The host name recorded in the snapshots (default: the system's). |
| `schedule` | yes | `every` or `daily`, and optional `jitter` (below). |
| `prune` | no | `connection`, the prune identity's own connection (required; the backup identity's is never used for it), `keep_last`, `keep_within`, `keep_daily`, `keep_weekly`, `keep_monthly`, `keep_yearly` (at least one) and `grace` (default 24h): the `prune` command's policy. Prune runs only after a backup run that succeeded. |
| `log_file` | no | Append the log to this file, created with no group or other permission, instead of standard error. Needed on Windows, where a service has no standard error. |
| `log_format` | no | `text` (default) or `json`. |
| `source_snapshot` | no | `method` (`none`, the default, `auto`, `lvm`, `btrfs`, `zfs`, `apfs` or `vss`), `on_failure` (`fail`, the default, or `fallback`), `dir` (the snapshot directory), `lvm.size` and `btrfs.dir`: see Source snapshots. An unknown method or an option for another method is refused. |

Durations are strings: a Go duration (`90m`, `6h`) or a whole number of days (`30d`).

### Schedule forms

- `"every": "6h"`: a fixed interval of at least 1m. The first run starts when the service starts; later runs stay on that grid.
- `"daily": "02:30"`: once a day at a local time of day, `HH:MM`, 24-hour, following daylight saving time.
- `"jitter": "30m"`: delays each run by a random time from 0 up to the jitter, which must be shorter than the period, so many hosts on one schedule do not start together.

Runs never overlap: a due time that passes while a run is still going is skipped, not queued. The scheduler rereads the wall clock at least once a minute, so a run missed while the machine slept starts once, within a minute of waking; due times missed during the sleep are not made up. The schedule keeps no state on disk: a daily run missed while the machine was off waits for the next day.

### Logging

Records are written with `log/slog`: `service started`, `next run`, `run started`, `snapshot saved` (source, snapshot ID, files, dirs, symlinks, skipped, bytes, new chunks and bytes, deduplicated chunks, packs written, duration), `backup failed`, `prune group` for each host and source directory (host, root, kept, forgotten), `prune finished` (kept, forgotten, marked, removed packs and bytes, partial packs, unused bytes, duration), `run finished` or `run failed` (snapshots, duration, error) and `service stopped`. With source snapshots on, each step is logged with its method, volume and snapshot name: `source snapshot created`, `mounted`, `unmounted`, `removed`, `leftover removed` and `leftover unmounted`, and warnings for `source snapshot sweep failed` and, under the fallback policy, `source snapshot failed; backing up the live sources`. No passphrase, key, token or credential is ever logged; errors name a secret's file, never its content.

### Linux (systemd)

`backup/packaging/systemd/objstore-backup.service` is a long-running service; the schedule is in the configuration, so there is no timer unit. It runs as the dedicated `objstore-backup` user with only `CAP_DAC_READ_SEARCH`, which lets it read every source and grants nothing else, under `NoNewPrivileges`, `ProtectSystem=strict` (the whole file system read-only), `ProtectHome=read-only`, `PrivateTmp`, `PrivateDevices`, a system call filter and the other directives in the file.

```
useradd --system --no-create-home --shell /usr/sbin/nologin objstore-backup
install -m 0755 objstore-backup /usr/local/bin/objstore-backup
install -d -m 0700 -o objstore-backup -g objstore-backup /etc/objstore-backup
install -m 0600 -o objstore-backup -g objstore-backup config.json /etc/objstore-backup/config.json
install -m 0400 -o objstore-backup -g objstore-backup passphrase /etc/objstore-backup/passphrase
install -m 0644 objstore-backup.service /etc/systemd/system/
systemctl daemon-reload && systemctl enable --now objstore-backup
journalctl -u objstore-backup -f
```

### macOS (launchd)

`backup/packaging/launchd/com.github.jeremyhahn.objstore-backup.plist` is a launch daemon: launchd starts it at boot and restarts it if it fails. It runs as root, which macOS needs to read other users' files; give the binary Full Disk Access (System Settings, Privacy and Security) to read the folders macOS protects.

```
sudo install -m 0755 objstore-backup /usr/local/bin/objstore-backup
sudo install -d -m 0700 /usr/local/etc/objstore-backup
sudo install -m 0600 config.json /usr/local/etc/objstore-backup/config.json
sudo install -m 0400 passphrase /usr/local/etc/objstore-backup/passphrase
sudo install -m 0644 com.github.jeremyhahn.objstore-backup.plist /Library/LaunchDaemons/
sudo launchctl bootstrap system /Library/LaunchDaemons/com.github.jeremyhahn.objstore-backup.plist
```

### Windows (service control manager)

The binary runs as a Windows service through `golang.org/x/sys/windows/svc` when the service control manager starts it, and in the foreground otherwise. It has no install subcommand: install it with `sc.exe`, from an elevated PowerShell. It runs as LocalSystem, which can read every source. Set `log_file` (for example `C:\\ProgramData\\objstore-backup\\service.log` in the JSON) and lock the directory down to SYSTEM and Administrators, so the configuration, passphrase and token files pass the ACL check:

```
New-Item -ItemType Directory C:\ProgramData\objstore-backup
icacls C:\ProgramData\objstore-backup /inheritance:r /grant:r "SYSTEM:(OI)(CI)F" "Administrators:(OI)(CI)F"
Copy-Item config.json, passphrase C:\ProgramData\objstore-backup\
sc.exe create objstore-backup start= delayed-auto binPath= "\"C:\Program Files\objstore-backup\objstore-backup.exe\" service --config C:\ProgramData\objstore-backup\config.json"
sc.exe description objstore-backup "objstore-backup scheduled backup agent"
sc.exe failure objstore-backup reset= 86400 actions= restart/30000/restart/30000/restart/30000
sc.exe start objstore-backup
```

To remove it: `sc.exe stop objstore-backup`, then `sc.exe delete objstore-backup`. A stop request cancels the current run and closes the repository before the service reports stopped.

## Source snapshots

A backup of a live file system can catch a file halfway through a write, or two files out of step. With `source_snapshot` in the service configuration, or `--snapshot <method>` on `backup`, each run snapshots every volume that holds a source, once per volume, reads the sources inside the snapshots, and removes them when the run ends: on success, on failure, on cancellation and when a panic unwinds the run. The result is crash consistent: what the volume would hold had the power failed at that instant. The snapshots record the original source paths, so a restore is the same as one of a backup without a snapshot. Code: `backup/pkg/srcsnap` (`Snapshotter.Create` and `Sweep`, `Set.Path` and `Close`), with `backup.Options.ReadFrom` pointing the walk into the snapshot.

| Method | Tool | What it does | Privileges |
|---|---|---|---|
| `lvm` | `lvs`, `lvcreate`, `mount`, `umount`, `lvremove` | `lvcreate --snapshot` of the source's logical volume, sized by `lvm.size` (`--snapshot-lvm-size`; a size such as `4G` or extents such as `10%ORIGIN`, the default), mounted read-only (`nouuid` for XFS) under the snapshot directory; cleanup unmounts and runs `lvremove`. A snapshot that fills its copy-on-write space becomes invalid and the run fails. | root (`CAP_SYS_ADMIN`) |
| `btrfs` | `btrfs` | A read-only `btrfs subvolume snapshot -r` of the subvolume that holds the source, made beside it or in `btrfs.dir` (`--snapshot-btrfs-dir`), which must be on the same file system; cleanup runs `btrfs subvolume delete`. When the subvolume is the mount point itself, as with btrfs at `/home` on another root file system, the default is a private `.objstore-backup-snapshots` directory at the top of that mount, which the run creates and removes when empty, the sweep covers, and the backup never reads. Nested subvolumes and other mounts below a source are not part of the snapshot. | root |
| `zfs` | `zfs` | `zfs snapshot` of the source's dataset, read through its `.zfs/snapshot/<name>` directory; cleanup runs `zfs destroy`. A source under a bind mount of a directory in a dataset is refused before any snapshot is made: the bind mount has no `.zfs`; name the source under the dataset's own mount point. | root, or `zfs allow` of snapshot, destroy and mount |
| `apfs` | `tmutil`, `mount_apfs`, `umount` | `tmutil localsnapshot`, then `mount_apfs -s` of that snapshot read-only under the snapshot directory; cleanup unmounts and runs `tmutil deletelocalsnapshots`. macOS only. | root, and Full Disk Access |
| `vss` | Windows PowerShell | The `Win32_ShadowCopy` CIM class: `Create` with the `ClientAccessible` context, then reading through the shadow copy's `\\?\GLOBALROOT\Device\HarddiskVolumeShadowCopyN\` device; cleanup removes the shadow copy. Windows only; Microsoft documents this class for Windows Server, not for client editions. | Administrator or LocalSystem |
| `auto` | as picked | Picks per volume by file system: btrfs, zfs, apfs, NTFS or ReFS to VSS, and ext2/3/4 or XFS to LVM when the device is a device-mapper device. | as picked |

The default, `none`, reads the live sources.

- **Tools.** Each tool is resolved once, by absolute path, from the system's tool directories (never `$PATH`), and run with an argument vector, never through a shell.
- **Names.** Every snapshot, mount point and record is named `objstore-backup-` and 16 random hex digits, checked before it is used. At service start and before each run, leftovers of an interrupted run that carry the prefix are removed and logged; nothing without the prefix is ever touched.
- **Snapshot directory.** `dir` (`--snapshot-dir`) holds the mount points, the records of APFS and VSS snapshots, and a lock. Default `/run/objstore-backup` on Linux, `/var/run/objstore-backup` on macOS and `%ProgramData%\objstore-backup\snapshots` on Windows. It must be private: on Linux and macOS, a directory owned by the agent's user that group and others cannot write; on Windows, one owned by SYSTEM, Administrators or the agent's user whose DACL grants no one else write, delete, permission or ownership change, and so must every record in it. A missing directory is made so on Windows, with a protected DACL; one that is not is refused before anything in it is read.
- **Host lock.** Every run also takes a host lock, in the default snapshot directory whatever `dir` is (on Windows, under the system's ProgramData folder), so it must exist or be creatable, private, for the agent's user even when `dir` is elsewhere. The ZFS and LVM sweeps remove prefixed snapshots anywhere on the host, and the btrfs one those in its snapshot directory, the hidden one included; the host lock keeps any sweep from running beside a live run, whatever directories the two use. One snapshotting run at a time per host: a second fails as busy, it does not wait.
- **Failure policy.** `on_failure` (`--snapshot-on-failure`) is `fail`, the default: a run whose snapshot cannot be made backs nothing up and fails. `fallback` logs a warning and backs the live sources up instead. Cancellation is never a fallback.
- **No fsfreeze.** There is no `fsfreeze` mode. Freezing a mounted file system blocks every writer on it for the whole backup, which can take hours, and a process that dies while it holds the freeze leaves the file system frozen until an operator thaws it; freezing the root file system can hang the host that would thaw it. A snapshot needs a consistent point only for an instant: LVM suspends the file system for that instant itself, btrfs, ZFS and APFS snapshots are atomic without one, and VSS coordinates with writers.
- **systemd.** The shipped unit runs with `CAP_DAC_READ_SEARCH` only, which cannot make snapshots. To use them, add a drop-in (`systemctl edit objstore-backup`) that runs the service as root, or keeps the user and adds `CAP_SYS_ADMIN` to `AmbientCapabilities` and `CapabilityBoundingSet`, and that sets `PrivateDevices=no` (LVM needs `/dev/mapper`), an empty `SystemCallFilter=` to clear the unit's filter (the tools mount and make privileged calls), `ReadWritePaths=` for the btrfs snapshot directory (`ProtectSystem=strict` leaves it read-only), and `RuntimeDirectory=objstore-backup` with `RuntimeDirectoryMode=0700` for the snapshot directory.

## Standards it builds on

| Concern | Standard | Use |
|---|---|---|
| Encryption | NIST SP 800-38D (AES-GCM), AES-256 | Authenticated encryption of every chunk pack, index and snapshot object |
| Key derivation | RFC 5869 / NIST SP 800-56C (HKDF-SHA-256) | Per-purpose subkeys (chunk IDs, object IDs, packs, index, snapshots, config) from the repository data key; the pack subkey, the busiest, nears the SP 800-38D §8.3 limit of 2^32 random-nonce invocations only after 2^32 sealed chunks |
| Passphrase keys | NIST SP 800-132 (PBKDF2-HMAC-SHA-256) | Standalone mode only, where no KMS is available; Argon2id and scrypt are not FIPS-approved |
| Key wrapping | Envelope encryption through the agent's key seam | The data key is wrapped by a KEK from go-xkms (KMS or HSM), or from a local key held in go-enclave locked memory |
| FIPS | FIPS 140-3 through Go's `crypto/fips140` | The agent builds and passes its tests under `make test-fips`, like the rest of the repository |
| Chunk identity | HMAC-SHA-256 (FIPS 198-1, FIPS 180-4) keyed with a repository secret | Content-addressed deduplication without exposing plaintext hashes, which allow confirmation-of-file attacks |
| Chunking | FastCDC content-defined chunking (Xia et al., USENIX ATC 2016) | Incremental at sub-file granularity |
| Compression | Zstandard, RFC 8878 | Compress, then encrypt; compression after encryption gains nothing |
| Backup practice | 3-2-1 rule; NIST SP 800-34 Rev. 1 (contingency planning) §3.4.2; NIST SP 800-209 (storage infrastructure security) | Off-host copy, restore testing (`verify`), retention, immutable destination |
| Ransomware resistance | S3 Object Lock, Azure immutable blob storage, GCS bucket lock | Packs are immutable and written once; prune runs from a separate identity |
| Consistency | Windows VSS; LVM, btrfs and ZFS snapshots on Linux; APFS snapshots on macOS | Optional snapshot of each source volume before the walk; no `fsfreeze` mode (see Source snapshots) |

## Not chosen

Recommending restic or kopia and serving restic's REST backend protocol from `objstore-server` was rejected. restic encrypts with AES-256-CTR plus Poly1305-AES and derives keys with scrypt, so it is not FIPS 140-3 capable and its keys cannot come from go-xkms. Kopia carries its own S3, GCS and Azure backends, which the constitution forbids a portfolio deliverable to duplicate. In both cases the binary is not ours: we cannot fix it, sign it or hold it to the portfolio's security suite, so it is not a go-objstore deliverable.

## Open item

zstd in the backup module's go.mod (`backup/go.mod`): `github.com/klauspost/compress` (a new third-party dependency of the `backup/` module; the standard library has no zstd) or stdlib gzip (RFC 1952), with no new dependency but a worse ratio and speed. The root module and the SDK module are unaffected either way. The owner decides. Until then the sealed format records a compression identifier and implements only `none`.

## Boundaries

- go-qrdb's database backup (`Client.Backup()` in the constitution's go-quicbaas surface) is a separate feature and is unaffected.
- The agent is its own module, `backup/`, inside go-objstore; it consumes the Go client SDK module (`api/sdks/go`) and go-enclave and nothing else first-party.
- go-objstore's root module keeps its single first-party dependency (go-enclave), and the SDK module gains none. go-xkms keys reach the agent only through the key seam, whose interface is defined in `backup/` and which go-xkms implements downward; go-objstore never imports go-xkms.
- The agent is a client only: it adds no endpoint and no change to `common.Storage`. The server gained only configurable gRPC message limits (`grpc.max_recv_msg_size`, `grpc.max_send_msg_size`), so backup packs fit in one message.
- Cloud credentials are never logged and never written into object metadata or the repository.
