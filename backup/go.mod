module github.com/jeremyhahn/go-objstore/backup

go 1.27.3

// The backup agent lives in the go-objstore repository and is built and tested
// against the in-tree Go client SDK and, through it, the in-tree parent module.
replace (
	github.com/jeremyhahn/go-objstore => ..
	github.com/jeremyhahn/go-objstore/api/sdks/go => ../api/sdks/go
)

require (
	github.com/jeremyhahn/go-enclave v0.11.1-alpha
	github.com/jeremyhahn/go-objstore v0.9.1-alpha
	github.com/jeremyhahn/go-objstore/api/sdks/go v0.9.0-alpha // held: replaced by the in-repo module ../api/sdks/go; its latest is the current main commit
	golang.org/x/sys v0.50.0
	google.golang.org/grpc v1.85.0
)

require (
	github.com/quic-go/qpack v0.6.0 // indirect
	github.com/quic-go/quic-go v0.65.0 // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/net v0.62.0 // indirect
	golang.org/x/text v0.44.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20261110000943-09e21b237f2c // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)
