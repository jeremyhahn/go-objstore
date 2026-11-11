# Server Authentication (`auth`)

The `auth` section of the server configuration turns on authentication and
role-based access control on every transport a server binary serves: gRPC,
REST, QUIC, MCP (HTTP and stdio) and the Unix socket. It is read from the
configuration file (see [Server Configuration](servers.md)); its lists have
no flag.

```yaml
auth:
  client_ca: /etc/objstore/client-ca.pem
  roles:
    backup: [read, write, list]
    prune: [delete]
  identities:
    - subject: backup-agent        # client certificate common name
      roles: [backup]
    - subject: prune-agent
      roles: [backup, prune]
    - subject: "uid:1000"          # local user on the Unix socket or MCP stdio
      roles: [backup]

grpc:
  tls_cert: /etc/objstore/server.crt
  tls_key: /etc/objstore/server.key
rest:
  tls_cert: /etc/objstore/server.crt
  tls_key: /etc/objstore/server.key
```

Setting any of `client_ca`, `roles` or `identities` turns `auth` on.

## Keys

| Key | Meaning |
|-----|---------|
| `auth.client_ca` | PEM file of the CA that signs client certificates. The gRPC, REST, QUIC and MCP HTTP listeners require a client certificate and verify it against this CA. One CA serves all listeners. |
| `auth.roles` | Map of a role name to the actions it permits: `read`, `write`, `delete`, `list`, `admin`, or `*` for all. |
| `auth.identities` | List of `{subject, roles}`. `subject` is a client certificate common name, or `uid:N` for a local user; `roles` names roles defined in `auth.roles`. |

`identities` is a list, not a map, because configuration keys are case-folded
and split at dots, which would change certificate names such as
`Backup.Agent`.

## Identities

- **gRPC, REST, QUIC, MCP HTTP**: the identity is the common name (CN) of the
  verified client certificate, matched exactly (case and all) against
  `subject`. Subject alternative names are not read, and no revocation check
  (CRL or OCSP) is made: to withdraw a certificate, remove its identity or
  replace `auth.client_ca`.
- **Unix socket**: the identity is `uid:N`, where `N` is the peer's user id read
  from the socket (`SO_PEERCRED`, Linux). The peer's OS group grants no role.
- **MCP stdio**: there is no credential on the stream; the identity is
  `uid:N` of the user that owns the server process.

A certificate whose CN has the form `uid:N` gets no roles: a `uid:N` subject
only matches a local user, never a certificate.

## Actions

A role grants actions only. It has no bucket, prefix or key scope: an
identity with `write` may write any key, and one with `delete` may delete
any key. `write` covers both creating an object and overwriting an existing
one, and updating its metadata.

On REST the action comes from the matched route and the HTTP method, never
from the request path, so an object key such as `a/metadata/b` or
`x/archive/y` needs the same action as any other key. A request that matches
no route is refused. Both route sets, with and without the `/api/v1` prefix,
need the same actions:

| REST endpoint | Action |
|---------------|--------|
| `GET /objects` | `list` |
| `GET`, `HEAD /objects/*key` | `read` |
| `PUT /objects/*key` | `write` |
| `DELETE /objects/*key` | `delete` |
| `HEAD /exists/*key` | `read` |
| `GET /metadata/*key` | `read` |
| `PUT /metadata/*key` | `write` |
| `POST /archive` | `admin` |
| `GET`, `POST /policies`, `DELETE /policies/*id`, `POST /policies/apply` | `admin` |
| every `/replication/...` route | `admin` |
| `GET /metrics` | `read` (none when the metrics endpoint is public) |
| `GET /health` | none (no authentication) |
| `GET /swagger/*any` | none (authentication only) |

gRPC maps each method by name: `Get`, `GetMetadata` and `Exists` need
`read`; `Put`, `PutStream` and `UpdateMetadata` `write`; `Delete` `delete`;
`List` `list`; `Archive` and every lifecycle and replication policy method
`admin`. `Health` needs no action: any authenticated identity may call it,
whatever its roles. A method not in that list needs `admin`. The standard
`grpc.health.v1.Health` service and server reflection skip authorization.

| gRPC method | Action |
|-------------|--------|
| `Health` | none (authentication only) |

## Fail-closed rules

With `auth` set, the server refuses to start, and reports every problem at
once, when:

- an enabled gRPC, REST, QUIC or MCP HTTP listener has no `tls_cert` and
  `tls_key` (a QUIC `self_signed` certificate is not enough);
- such a listener is enabled and `auth.client_ca` is not set;
- `auth.roles` is empty, a role lists no action, or lists an unknown action;
- an identity has an empty or duplicate `subject`, a `uid:N` subject whose `N`
  is not a non-negative integer, no roles, or a role not defined in
  `auth.roles`;
- an identity's `subject` is `anonymous`, the principal a transport gives a
  peer it cannot identify;
- a listener's TLS settings fail to build (for example `auth.client_ca`
  cannot be read): the listener returns the error instead of serving without
  TLS.

At run time:

- a connection without a client certificate, or with one not signed by
  `auth.client_ca`, is refused in the TLS handshake;
- a client that matches no identity authenticates with no roles, so every
  request it makes is refused (HTTP 403, gRPC `PermissionDenied`, JSON-RPC
  `-32001` forbidden);
- a request whose action none of the identity's roles permits is refused the
  same way; a Unix peer whose credentials cannot be read has no roles.

## Without `auth`

With no `auth` section the servers behave as before: no client certificate
is asked for and every client may do everything. Each listener logs a warning
at startup:

```
auth is not configured: listener serves unauthenticated, every client may do everything
```

`tls_cert` and `tls_key` alone still give server-side TLS on the listener,
without client certificates. Keep unauthenticated listeners on loopback (the
default) or behind a trusted proxy.
