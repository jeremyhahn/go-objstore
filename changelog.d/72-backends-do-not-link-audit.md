### Changed

- Importing a storage backend no longer links gin, the mongo bson codec or
  ugorji (issue #72). `pkg/local` imported `pkg/audit`, whose HTTP and gRPC
  middleware pulled those into every consumer (go-xkms `xkey` among them). The
  audit contract (`AuditLogger`, `AuditEvent`, `EventType` and its constants,
  `Result` and its constants, `NoOpAuditLogger`, `NewNoOpAuditLogger`) moved to
  the new dependency-free `pkg/auditlog`; `pkg/audit` keeps the slog-backed
  logger and the middleware and implements that interface. Code that names the
  moved identifiers changes its import from `pkg/audit` to `pkg/auditlog`
  (`audit.AuditLogger` is now `auditlog.AuditLogger`, and so on); the
  constructors `audit.NewDefaultAuditLogger`, `audit.NewAuditLogger` and the
  middleware are unchanged. Behaviour is unchanged. A test in
  `internal/depguard` fails if any backend package's dependency graph includes
  gin, mongo-driver, ugorji, `pkg/audit` or `pkg/server`.
