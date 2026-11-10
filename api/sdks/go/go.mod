module github.com/jeremyhahn/go-objstore/api/sdks/go

go 1.27.3

require (
	github.com/jeremyhahn/go-objstore v0.9.1-alpha
	github.com/quic-go/quic-go v0.65.0
	github.com/stretchr/testify v1.12.1
	google.golang.org/grpc v1.85.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/quic-go/qpack v0.6.0 // indirect
	github.com/stretchr/objx v0.8.0 // indirect
	go.uber.org/mock v0.6.0 // indirect
	go.yaml.in/yaml/v3 v3.0.6 // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/net v0.62.0 // indirect
	golang.org/x/sys v0.50.0 // indirect
	golang.org/x/text v0.44.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20261110000943-09e21b237f2c // indirect
)

// This SDK lives in the go-objstore monorepo and is built/tested against the
// in-tree parent module. The published v0.1.2-alpha tag is a placeholder.
replace github.com/jeremyhahn/go-objstore => ../../../
