// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed:
//
// 1. GNU Affero General Public License v3.0 (AGPL-3.0)
//    See LICENSE file or visit https://www.gnu.org/licenses/agpl-3.0.html
//
// 2. Commercial License
//    Contact licensing@automatethethings.com for commercial licensing options.

//go:build awss3

package s3

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/iotest"

	"github.com/jeremyhahn/go-objstore/pkg/common"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// partRecord is what an UploadPart request looked like on the wire.
type partRecord struct {
	auth          string
	contentSHA256 string
	contentLength int64
	query         string
	body          []byte
}

// partServer accepts UploadPart requests and records them.
type partServer struct {
	*httptest.Server
	mu   sync.Mutex
	reqs []partRecord
}

func (p *partServer) requests() []partRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]partRecord(nil), p.reqs...)
}

func newPartServer(t *testing.T, tls bool) *partServer {
	t.Helper()
	p := &partServer{}
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Only a request whose body arrived whole would be stored by the service.
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return
		}
		p.mu.Lock()
		p.reqs = append(p.reqs, partRecord{
			auth:          r.Header.Get("Authorization"),
			contentSHA256: r.Header.Get("X-Amz-Content-Sha256"),
			contentLength: r.ContentLength,
			query:         r.URL.RawQuery,
			body:          body,
		})
		p.mu.Unlock()
		w.Header().Set("ETag", `"part-etag"`)
	})
	if tls {
		p.Server = httptest.NewTLSServer(h)
	} else {
		p.Server = httptest.NewServer(h)
	}
	t.Cleanup(p.Close)
	return p
}

// signingS3 is an S3 over a real client (static credentials, v4 signer, no
// retries) pointed at srv, so a request body goes through the same signing
// pipeline as against the service.
func signingS3(t *testing.T, srv *partServer) *S3 {
	t.Helper()
	opts := s3.Options{
		Region:           "us-east-1",
		BaseEndpoint:     aws.String(srv.URL),
		UsePathStyle:     true,
		RetryMaxAttempts: 1,
		Credentials:      credentials.NewStaticCredentialsProvider("AKIDEXAMPLE", "example-secret-access-key", ""),
	}
	if strings.HasPrefix(srv.URL, "https://") {
		opts.HTTPClient = srv.Client()
	}
	return &S3{svc: s3.New(opts), bucket: "bucket-one"}
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// onlyReader hides every method but Read.
type onlyReader struct{ io.Reader }

func TestS3_UploadPart_SeekableBodyIsSigned(t *testing.T) {
	const payload = "the part payload"
	tests := []struct {
		name string
		r    func() io.Reader
		want string
	}{
		{"strings reader", func() io.Reader { return strings.NewReader(payload) }, payload},
		{"reader past an offset", func() io.Reader {
			r := strings.NewReader("skipped:" + payload)
			if _, err := r.Seek(int64(len("skipped:")), io.SeekStart); err != nil {
				t.Fatal(err)
			}
			return r
		}, payload},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newPartServer(t, false)
			s := signingS3(t, srv)
			r := tt.r()
			size := int64(len(payload))
			part, err := s.UploadPart(context.Background(), "k", "upload-1", 2, r, size)
			if err != nil {
				t.Fatalf("UploadPart = %v", err)
			}
			if part.PartNumber != 2 || part.Size != size || part.ETag != `"part-etag"` {
				t.Errorf("part = %+v", part)
			}
			got := srv.requests()
			if len(got) != 1 {
				t.Fatalf("%d requests", len(got))
			}
			if string(got[0].body) != payload || got[0].contentLength != size {
				t.Errorf("sent %q (length %d)", got[0].body, got[0].contentLength)
			}
			if !strings.Contains(got[0].auth, "AWS4-HMAC-SHA256") {
				t.Errorf("request is not v4 signed: %q", got[0].auth)
			}
			if got[0].contentSHA256 != sha256Hex([]byte(payload)) {
				t.Errorf("payload hash = %q, want the sha256 of the part", got[0].contentSHA256)
			}
			if !strings.Contains(got[0].query, "partNumber=2") || !strings.Contains(got[0].query, "uploadId=upload-1") {
				t.Errorf("query = %q", got[0].query)
			}
		})
	}
}

func TestS3_UploadPart_SeekableBodyShorterThanDeclared(t *testing.T) {
	srv := newPartServer(t, false)
	s := signingS3(t, srv)
	_, err := s.UploadPart(context.Background(), "k", "u", 1, strings.NewReader("abc"), 8)
	if !errors.Is(err, common.ErrInvalidArgument) {
		t.Errorf("err = %v, want a size mismatch", err)
	}
	if n := len(srv.requests()); n != 0 {
		t.Errorf("%d requests reached the server", n)
	}
}

func TestS3_UploadPart_SeekableBodyLongerThanDeclared(t *testing.T) {
	srv := newPartServer(t, false)
	s := signingS3(t, srv)
	_, err := s.UploadPart(context.Background(), "k", "u", 1, strings.NewReader("abcdefgh"), 4)
	if !errors.Is(err, common.ErrInvalidArgument) {
		t.Errorf("err = %v, want a size mismatch", err)
	}
	if n := len(srv.requests()); n != 0 {
		t.Errorf("%d requests reached the server", n)
	}
}

func TestS3_UploadPart_SeekableBodyReadError(t *testing.T) {
	boom := errors.New("boom")
	srv := newPartServer(t, false)
	s := signingS3(t, srv)
	r := readSeeker{Reader: iotest.ErrReader(boom), Seeker: strings.NewReader("abcd")}
	if _, err := s.UploadPart(context.Background(), "k", "u", 1, r, 4); !errors.Is(err, boom) {
		t.Errorf("err = %v, want the reader's failure", err)
	}
	if n := len(srv.requests()); n != 0 {
		t.Errorf("%d requests reached the server", n)
	}
}

type readSeeker struct {
	io.Reader
	io.Seeker
}

func TestS3_UploadPart_StreamedBodyIsUnsignedOverTLS(t *testing.T) {
	const payload = "streamed part payload"
	srv := newPartServer(t, true)
	s := signingS3(t, srv)
	size := int64(len(payload))
	part, err := s.UploadPart(context.Background(), "k", "upload-1", 1, onlyReader{strings.NewReader(payload)}, size)
	if err != nil {
		t.Fatalf("UploadPart = %v", err)
	}
	if part.Size != size {
		t.Errorf("part = %+v", part)
	}
	got := srv.requests()
	if len(got) != 1 {
		t.Fatalf("%d requests", len(got))
	}
	if string(got[0].body) != payload || got[0].contentLength != size {
		t.Errorf("sent %q (length %d)", got[0].body, got[0].contentLength)
	}
	if !strings.Contains(got[0].auth, "AWS4-HMAC-SHA256") {
		t.Errorf("request is not v4 signed: %q", got[0].auth)
	}
	if got[0].contentSHA256 != "UNSIGNED-PAYLOAD" {
		t.Errorf("payload hash = %q, want UNSIGNED-PAYLOAD", got[0].contentSHA256)
	}
}

func TestS3_UploadPart_UnseekableSeekerIsStreamed(t *testing.T) {
	// A reader that has a Seek method but cannot seek (a pipe, say) is streamed.
	const payload = "piped"
	srv := newPartServer(t, true)
	s := signingS3(t, srv)
	r := readSeeker{Reader: strings.NewReader(payload), Seeker: failingSeeker{errors.New("illegal seek")}}
	if _, err := s.UploadPart(context.Background(), "k", "u", 1, r, int64(len(payload))); err != nil {
		t.Fatal(err)
	}
	got := srv.requests()
	if len(got) != 1 || string(got[0].body) != payload || got[0].contentSHA256 != "UNSIGNED-PAYLOAD" {
		t.Errorf("requests = %+v", got)
	}
}

func TestS3_UploadPart_StreamedBodySizeMismatchOverTLS(t *testing.T) {
	srv := newPartServer(t, true)
	s := signingS3(t, srv)
	_, err := s.UploadPart(context.Background(), "k", "u", 1, onlyReader{strings.NewReader("abc")}, 8)
	if !errors.Is(err, common.ErrInvalidArgument) {
		t.Errorf("err = %v, want a size mismatch", err)
	}
}

func TestS3_UploadPart_StreamedBodyRefusedOverPlainHTTP(t *testing.T) {
	srv := newPartServer(t, false)
	s := signingS3(t, srv)
	_, err := s.UploadPart(context.Background(), "k", "u", 1, onlyReader{strings.NewReader("abc")}, 3)
	if !errors.Is(err, common.ErrInvalidArgument) {
		t.Fatalf("err = %v, want a refusal", err)
	}
	if n := len(srv.requests()); n != 0 {
		t.Errorf("%d requests reached the server", n)
	}
}
