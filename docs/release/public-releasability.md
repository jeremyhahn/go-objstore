# Public releasability

> **Status: NORMATIVE** · Scope: go-objstore · Last verified: 2026-10-21
> What the public release of this repository ships, and the gate that keeps it true.

Everything tracked here is published except the private development workflow,
which `.gitattributes` excludes (below). This page is the rule and the gates
that enforce it. Conflicts with this document are defects.

## The rule

No published file names the private development fleet or its owner. Four
classes:

| Class | What it covers | Use instead |
|---|---|---|
| `lab-address` | An address on the fleet subnet | RFC 5737 documentation addresses: `192.0.2.0/24`, `198.51.100.0/24`, `203.0.113.0/24` |
| `lab-host` | A fleet hostname | The role, not the machine: `server.example.com`, `agent-01`, `build-01.example` |
| `owner-path` | A path under the owner's home directory | A repository-relative path, or `/var/lib`, `/srv`, `/home/agent` |
| `owner-account` | The owner's account name on its own | A role (`operator`, `agent`) or the module path |

The fleet's values are not in the tree (IS-31). The rules load at test time
from the file named by the `LABSTRINGS_PATTERNS` environment variable; in CI
that file is the `RELEASABILITY_PATTERNS` secret. With the variable unset there
are no rules and the gate finds nothing. Nothing the gate prints, a finding, a
subtest name or a loader error, carries a rule or a matched value.

Not a violation: RFC 1918 literals that are not the fleet subnet (tests use
`192.168.1.x` as an opaque address that is never dialed), and the module path
`github.com/jeremyhahn/...`: the module and GitHub owner is public.

## Development tooling stays private

`CLAUDE.md` and `AGENTS.md` (at any depth), `.claude/` and `.serena/` are the
agent development workflow. `.gitattributes` marks each `export-ignore`, which
every archive git builds applies (`git archive`, a forge's tag tarball).
`.dockerignore` carries the same four entries. The gate does not scan them.

Not covered: the Go module zip and a push of this repository's history do not
honour `export-ignore`. The public tree must be produced as an export
(`git archive` of the release commit).

## The gate

`internal/labstrings` is test-only. It scans every published file (`git
ls-files` minus the export-ignored paths) and reports each offending line with
its file, line, class and what to use instead (never the matched text). It runs as
`TestTheTrackedTreeIsPubliclyReleasable` inside `make test` and CI's unit job;
there is no separate target.

```
LABSTRINGS_PATTERNS=/abs/path/to/patterns.tsv go test ./internal/labstrings/
```

Other tests cover the patterns, the waiver, the allow-list, the manifest, the
`.gitattributes` export-ignore list (checked with a real `git archive`) and the
`.dockerignore` entries.

### Accepting a line

Prefer rewriting the line. A waiver is for a line whose purpose is the value
itself: an inline comment on the line, or the
line above, in the file's own comment syntax:

```
lab-string-ok: <reason>
```

The reason is what makes it a waiver; `lab-string-ok:` with nothing after it is
not one. The second mechanism is `defaultAllowList`, for a file that cannot carry
a comment. Every entry needs a reason and may only name the gate's own files
(`internal/labstrings/`). It is empty today.

### Adding a fleet value

Add one line to the patterns file (and the `RELEASABILITY_PATTERNS` secret),
never to the tree. One rule per line, `class<TAB>regexp`; blank lines and `#`
lines are skipped. The class is one of the four above. The path must be
absolute and name a regular file (not a symlink) of at most 1 MiB. An unknown
class, an empty regexp or one that does not compile fails the gate with the
file and line number only. The scan's needle and the guidance are derived from
the rule, not written by hand.
