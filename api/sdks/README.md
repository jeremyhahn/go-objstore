# go-objstore SDK Suite

Multi-language client SDKs for the go-objstore object storage service.

**Status: all SDK unit suites pass in Docker, with comprehensive, aligned test
coverage across every language.**

## Overview

Six SDKs spanning three network protocols, with a unified client per language
that auto-selects the best available protocol. Each SDK exposes three protocol
clients (REST, gRPC, QUIC) plus a unified client.

The TypeScript SDK is published as `@go-objstore/client` and ships compiled
JavaScript (ESM + CJS) plus type declarations, so plain JavaScript projects can
consume it directly (`require('@go-objstore/client')`). There is no separate
JavaScript SDK.

The table below shows verified unit test counts from running the suites in
Docker containers. Each SDK's protocol clients have >=90% line coverage.

| Language   | REST | gRPC | QUIC | Unified | Unit Tests                     | Coverage              |
|------------|------|------|------|---------|--------------------------------|-----------------------|
| Python     | yes  | yes  | yes  | yes     | 351                            | >=90% (protocol clients) |
| Ruby       | yes  | yes  | yes  | yes     | 211 examples                   | >=90% (protocol clients) |
| Go         | yes  | yes  | yes  | yes     | pass                           | ~94% (client)         |
| Rust       | yes  | yes  | yes  | yes     | 183 lib + 47 unit_tests        | >=90% (protocol clients) |
| TypeScript | yes  | yes  | yes  | yes     | 245                            | >=90% (protocol clients) |
| C#         | yes  | yes  | yes  | yes     | 179                            | >=90% (protocol clients) |

Rust's 4 network-dependent unified tests are `#[ignore]`'d in the unit run and
exercised in the integration suite.

## Service Operations

All SDKs implement the same 19 canonical operations plus a connection-close
method (Close/Dispose):

- **Objects**: Put, Get, Delete, List, Exists
- **Metadata**: GetMetadata, UpdateMetadata
- **Service**: Health
- **Archival**: Archive
- **Lifecycle Policies**: AddPolicy, RemovePolicy, GetPolicies, ApplyPolicies
- **Replication**: AddReplicationPolicy, RemoveReplicationPolicy,
  GetReplicationPolicies, GetReplicationPolicy, TriggerReplication,
  GetReplicationStatus

## Quick Start

Each SDK lives in its own directory with language-specific instructions:

```
api/sdks/python/      # pip install
api/sdks/ruby/        # gem install
api/sdks/go/          # go get
api/sdks/rust/        # cargo add
api/sdks/typescript/  # npm install @go-objstore/client (TypeScript + JavaScript)
api/sdks/csharp/      # dotnet add
```

## Testing

Every SDK follows the same canonical test matrix, so all languages exercise the
same behaviors consistently. Per protocol (REST / gRPC / QUIC): each of the 19
operations gets a success and an error case, the 9 mutating operations get a
not-found case, plus a metadata round-trip test and an empty-key validation
test. Unified-client delegation and close/dispose tests round out the suite,
yielding roughly 150-170 aligned unit tests per SDK.

All SDKs are tested in Docker containers, so no host language toolchains are
required:

```bash
cd api/sdks && make test              # All unit tests, all languages
cd api/sdks && make integration-test  # Integration tests against live server
```

`make test` reports `ALL SDK Unit Tests Passed`. Integration tests run against
a live server container.

## Metadata

Custom object metadata is a JSON string-to-string map of custom keys only, and
travels on the wire differently per protocol:

- **REST**: the `X-Object-Metadata` HTTP header (a JSON string-to-string map of
  custom keys). Content-Type and Content-Encoding travel as standard HTTP
  headers.
- **QUIC**: `X-Meta-<key>` headers (one per custom key).
- **gRPC**: protobuf message fields.

## License

See repository root.
