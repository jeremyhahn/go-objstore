# Configuration: Using It

> **Status: NORMATIVE** · Scope: go-objstore · Last verified: 2026-10-09
> Conflicts with this document are defects. Fix them here first.

Task-oriented guide to configuring a go-objstore server or the CLI.

## Prerequisites

- An `objstore-*` server binary or the `objstore` CLI.
- For cloud backends, credentials from the provider's usual sources.

## First run

Start the combined server on the local filesystem backend:

```bash
objstore-server -backend local -path /var/lib/objstore
```

With no other settings it uses the defaults in [servers.md](servers.md).

## Common tasks

**Change a setting.** Pick any one layer. Flag beats environment beats file beats default.

```bash
objstore-server -rest-host 10.0.0.5
OBJSTORE_REST_HOST=10.0.0.5 objstore-server
```

**Use a config file.** Copy `configs/objstore-server.yaml.example`, edit it, and name it:

```bash
objstore-server -config /etc/objstore/objstore.yaml
# or: OBJSTORE_CONFIG=/etc/objstore/objstore.yaml objstore-server
```

**Turn a transport off or on.** `-grpc`, `-rest`, `-quic`, `-mcp`, `-unix` (the Unix socket is off by default).

**Add TLS.** Set the transport's certificate and key together (`-grpc-tls-cert` and `-grpc-tls-key`, and the equivalents for QUIC). For QUIC testing only, `-quic-self-signed`.

**Keep secrets out of files.** Supply credentials through environment variables or the cloud provider's credential chain, not flags or committed files.

**Configure the CLI.** See [cli.md](cli.md).

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| Server exits at startup naming a key | Unknown or mistyped key in the file, or a value out of range. Fix the named key. |
| Server exits naming a missing file | `-config` or `OBJSTORE_CONFIG` names a file that does not exist. |
| A transport does not start with TLS set | The certificate and key must both be given and loadable. There is no plaintext fallback. |
| A flag seems ignored | A flag overrides lower layers only when passed on the command line. Check the flag name against `-h`. |
| Backend unreachable | Run `objstore health` and check the credentials with the provider's own tool. |
