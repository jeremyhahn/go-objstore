module github.com/jeremyhahn/go-objstore/examples/encryption

go 1.25.3

require (
	github.com/jeremyhahn/go-keychain v0.1.5-alpha
	github.com/jeremyhahn/go-objstore v0.0.0
)

require (
	github.com/google/go-tpm v0.9.6 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/miekg/pkcs11 v1.1.1 // indirect
	github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78 // indirect
	golang.org/x/crypto v0.42.0 // indirect
	golang.org/x/sys v0.37.0 // indirect
)

replace github.com/jeremyhahn/go-objstore => ../..

