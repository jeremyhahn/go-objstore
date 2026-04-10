# go-objstore

Multi-backend object storage library supporting AWS S3, Azure Blob, Google Cloud Storage, local filesystem, and Glacier. Used by go-xkms, go-trusted-ca, and go-quicbaas for persistent object storage.

# Rules

- Follow software department rules in /home/jhahn/sources/automatethethings/software/CLAUDE.md
- No local project dependencies — this is a foundational library
- All backend implementations must satisfy the common ObjectStore interface
- Cloud credentials must never be logged or stored in object metadata
