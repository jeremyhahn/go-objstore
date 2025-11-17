#!/bin/bash
# Generate self-signed certificates for testing

set -e

CERT_DIR="./certs"
mkdir -p "$CERT_DIR"

# Generate private key
openssl genrsa -out "$CERT_DIR/server.key" 2048

# Generate certificate signing request
openssl req -new -key "$CERT_DIR/server.key" -out "$CERT_DIR/server.csr" \
    -subj "/C=US/ST=Test/L=Test/O=Test/CN=localhost"

# Generate self-signed certificate
openssl x509 -req -days 365 -in "$CERT_DIR/server.csr" \
    -signkey "$CERT_DIR/server.key" -out "$CERT_DIR/server.crt" \
    -extfile <(printf "subjectAltName=DNS:localhost,DNS:quic-server,DNS:rest-server,DNS:grpc-server,IP:127.0.0.1")

# Clean up CSR
rm "$CERT_DIR/server.csr"

# Set permissions
chmod 644 "$CERT_DIR/server.crt"
chmod 600 "$CERT_DIR/server.key"

echo "Certificates generated in $CERT_DIR"
