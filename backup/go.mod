module github.com/jeremyhahn/go-objstore/backup

go 1.27.3

// The backup agent lives in the go-objstore repository and is built and tested
// against the in-tree Go client SDK and, through it, the in-tree parent module.
replace (
	github.com/jeremyhahn/go-objstore => ..
	github.com/jeremyhahn/go-objstore/api/sdks/go => ../api/sdks/go
)

require (
	github.com/jeremyhahn/go-enclave v0.9.0-alpha
	github.com/jeremyhahn/go-objstore/api/sdks/go v0.0.0-00010101000000-000000000000
)

require (
	github.com/jeremyhahn/go-objstore v0.1.4-alpha // indirect
	github.com/quic-go/qpack v0.6.0 // indirect
	github.com/quic-go/quic-go v0.61.0 // indirect
	golang.org/x/crypto v0.57.0 // indirect
	golang.org/x/net v0.60.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/text v0.42.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260810153831-ec0a7760b754 // indirect
	google.golang.org/grpc v1.83.2 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)
