module github.com/jeremyhahn/go-objstore/api/sdks/go

go 1.26.5

require (
	github.com/jeremyhahn/go-objstore v0.1.4-alpha
	github.com/quic-go/quic-go v0.61.0
	github.com/stretchr/testify v1.12.0
	google.golang.org/grpc v1.83.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/kr/text v0.2.0 // indirect
	github.com/quic-go/qpack v0.6.0 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	github.com/stretchr/objx v0.5.3 // indirect
	go.uber.org/mock v0.6.0 // indirect
	golang.org/x/crypto v0.55.0 // indirect
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260810153831-ec0a7760b754 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// This SDK lives in the go-objstore monorepo and is built/tested against the
// in-tree parent module. The published v0.1.2-alpha tag is a placeholder.
replace github.com/jeremyhahn/go-objstore => ../../../
