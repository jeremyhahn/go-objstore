module github.com/jeremyhahn/go-objstore/examples/encryption

go 1.27.3

require github.com/jeremyhahn/go-objstore v0.9.0-alpha // held: replaced by the in-repo module ../..; the release does not tag this example

require (
	github.com/jeremyhahn/go-enclave v0.11.1-alpha // indirect
	golang.org/x/sys v0.50.0 // indirect
)

replace github.com/jeremyhahn/go-objstore => ../..
