//go:build e2esmoke

// Package objstore e2e smoke: exercises the MCP and Unix transports against a
// live server. Run with: go test -tags e2esmoke -run TestE2ESmoke -v
package objstore

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"
)

func TestE2ESmoke(t *testing.T) {
	mcpAddr := os.Getenv("SMOKE_MCP_ADDR")   // e.g. 127.0.0.1:18081
	unixSock := os.Getenv("SMOKE_UNIX_SOCK") // e.g. /tmp/objstore-test.sock
	if mcpAddr == "" || unixSock == "" {
		t.Skip("SMOKE_MCP_ADDR / SMOKE_UNIX_SOCK not set")
	}

	cases := []struct {
		name string
		cfg  ClientConfig
	}{
		{"mcp", ClientConfig{Protocol: ProtocolMCP, Address: mcpAddr}},
		{"unix", ClientConfig{Protocol: ProtocolUnix, Address: unixSock}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			cfg := tc.cfg
			c, err := NewClient(&cfg)
			if err != nil {
				t.Fatalf("NewClient(%s): %v", tc.name, err)
			}
			defer c.Close()

			key := "smoke/" + tc.name + "/obj.txt"
			payload := []byte("hello from " + tc.name + " transport")

			if _, err := c.Put(ctx, key, payload, &Metadata{ContentType: "text/plain"}); err != nil {
				t.Fatalf("Put: %v", err)
			}
			ok, err := c.Exists(ctx, key)
			if err != nil || !ok {
				t.Fatalf("Exists: ok=%v err=%v", ok, err)
			}
			got, err := c.Get(ctx, key)
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if !bytes.Equal(got.Data, payload) {
				t.Fatalf("round-trip mismatch: got %q want %q", got.Data, payload)
			}
			res, err := c.List(ctx, &ListOptions{Prefix: "smoke/" + tc.name})
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			if len(res.Objects) == 0 {
				t.Fatalf("List returned no objects under prefix")
			}
			if err := c.Delete(ctx, key); err != nil {
				t.Fatalf("Delete: %v", err)
			}
			ok, err = c.Exists(ctx, key)
			if err != nil {
				t.Fatalf("Exists after delete: %v", err)
			}
			if ok {
				t.Fatalf("object still exists after delete")
			}
			t.Logf("%s transport round-trip OK", tc.name)
		})
	}
}
