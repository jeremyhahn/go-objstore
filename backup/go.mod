module github.com/jeremyhahn/go-objstore/backup

go 1.27.3

// The backup agent lives in the go-objstore repository and is built and tested
// against the in-tree Go client SDK and, through it, the in-tree parent module.
replace (
	github.com/jeremyhahn/go-objstore => ..
	github.com/jeremyhahn/go-objstore/api/sdks/go => ../api/sdks/go
)

require (
	github.com/jeremyhahn/go-enclave v0.10.0-alpha
	github.com/jeremyhahn/go-objstore v0.8.0-alpha
	github.com/jeremyhahn/go-objstore/api/sdks/go v0.0.0-00010101000000-000000000000 // held: replaced by the in-repo module ../api/sdks/go; its latest is the current main commit
	golang.org/x/sys v0.49.0
	google.golang.org/grpc v1.84.0
)

require (
	github.com/quic-go/qpack v0.6.0 // indirect
	github.com/quic-go/quic-go v0.64.1 // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/net v0.61.0 // indirect
	golang.org/x/text v0.43.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20261026165753-2ffab32bce7e // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)
