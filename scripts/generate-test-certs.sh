#!/bin/bash

# Script to generate self-signed certificates for QUIC/HTTP3 testing
# DO NOT USE IN PRODUCTION

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CERT_DIR="${CERT_DIR:-$SCRIPT_DIR/../testdata/certs}"

# Create certificate directory
mkdir -p "$CERT_DIR"

# Certificate details
DAYS_VALID=365
KEY_SIZE=2048
COUNTRY="US"
STATE="CA"
CITY="San Francisco"
ORG="go-objstore Test"
CN="localhost"

echo "Generating test certificates for QUIC/HTTP3..."
echo "Certificate directory: $CERT_DIR"

# Generate private key
echo "Generating private key..."
openssl genrsa -out "$CERT_DIR/server-key.pem" $KEY_SIZE 2>/dev/null

# Generate certificate signing request
echo "Generating certificate signing request..."
openssl req -new \
    -key "$CERT_DIR/server-key.pem" \
    -out "$CERT_DIR/server.csr" \
    -subj "/C=$COUNTRY/ST=$STATE/L=$CITY/O=$ORG/CN=$CN" \
    2>/dev/null

# Create extensions file for SAN (Subject Alternative Name)
cat > "$CERT_DIR/extensions.cnf" <<EOF
[v3_req]
basicConstraints = CA:FALSE
keyUsage = nonRepudiation, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = *.localhost
IP.1 = 127.0.0.1
IP.2 = ::1
EOF

# Generate self-signed certificate
echo "Generating self-signed certificate..."
openssl x509 -req \
    -days $DAYS_VALID \
    -in "$CERT_DIR/server.csr" \
    -signkey "$CERT_DIR/server-key.pem" \
    -out "$CERT_DIR/server-cert.pem" \
    -extensions v3_req \
    -extfile "$CERT_DIR/extensions.cnf" \
    2>/dev/null

# Generate CA certificate for client verification (optional)
echo "Generating CA certificate..."
openssl genrsa -out "$CERT_DIR/ca-key.pem" $KEY_SIZE 2>/dev/null

openssl req -new -x509 \
    -days $DAYS_VALID \
    -key "$CERT_DIR/ca-key.pem" \
    -out "$CERT_DIR/ca-cert.pem" \
    -subj "/C=$COUNTRY/ST=$STATE/L=$CITY/O=$ORG/CN=$ORG Root CA" \
    2>/dev/null

# Generate client certificate (for mTLS testing)
echo "Generating client certificate..."
openssl genrsa -out "$CERT_DIR/client-key.pem" $KEY_SIZE 2>/dev/null

openssl req -new \
    -key "$CERT_DIR/client-key.pem" \
    -out "$CERT_DIR/client.csr" \
    -subj "/C=$COUNTRY/ST=$STATE/L=$CITY/O=$ORG/CN=client" \
    2>/dev/null

openssl x509 -req \
    -days $DAYS_VALID \
    -in "$CERT_DIR/client.csr" \
    -CA "$CERT_DIR/ca-cert.pem" \
    -CAkey "$CERT_DIR/ca-key.pem" \
    -CAcreateserial \
    -out "$CERT_DIR/client-cert.pem" \
    2>/dev/null

# Clean up temporary files
rm -f "$CERT_DIR/server.csr" "$CERT_DIR/client.csr" "$CERT_DIR/extensions.cnf" "$CERT_DIR/ca-cert.srl"

# Verify certificates
echo ""
echo "Verifying certificates..."
openssl x509 -in "$CERT_DIR/server-cert.pem" -noout -text | grep -A2 "Subject Alternative Name" || true
openssl verify -CAfile "$CERT_DIR/ca-cert.pem" "$CERT_DIR/client-cert.pem" 2>/dev/null || true

echo ""
echo "Certificate generation complete!"
echo ""
echo "Generated files:"
echo "  - Server certificate: $CERT_DIR/server-cert.pem"
echo "  - Server private key: $CERT_DIR/server-key.pem"
echo "  - CA certificate: $CERT_DIR/ca-cert.pem"
echo "  - CA private key: $CERT_DIR/ca-key.pem"
echo "  - Client certificate: $CERT_DIR/client-cert.pem"
echo "  - Client private key: $CERT_DIR/client-key.pem"
echo ""
echo "To use with QUIC server:"
echo "  -tlscert=$CERT_DIR/server-cert.pem -tlskey=$CERT_DIR/server-key.pem"
echo ""
echo "WARNING: These are test certificates only. DO NOT USE IN PRODUCTION!"
