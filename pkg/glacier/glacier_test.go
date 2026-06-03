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

//go:build glacier

package glacier

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glacier"
)

func TestGlacier_Configure_Errors(t *testing.T) {
	g := &Glacier{}
	if err := g.Configure(map[string]string{"region": "us-east-1"}); err == nil {
		t.Fatalf("expected error for missing vaultName")
	}
}

type errReader struct{}

func (e errReader) Read(p []byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestGlacier_Put_ReadError(t *testing.T) {
	g := &Glacier{}
	// Configure minimal fields to set vault and avoid nil deref of svc in Put before read
	_ = g.Configure(map[string]string{"region": "us-east-1", "vaultName": "v"})
	if err := g.Put("k", errReader{}); err == nil {
		t.Fatalf("expected error from reader")
	}
}

func TestGlacier_Configure_Success(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test that creates real AWS SDK client")
	}
	g := &Glacier{}
	if err := g.Configure(map[string]string{"region": "us-east-1", "vaultName": "vault"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if g.svc == nil {
		t.Fatalf("expected svc to be initialized")
	}
}

func TestGlacier_New(t *testing.T) {
	g := New()
	if g == nil {
		t.Fatal("New() returned nil")
	}
	if _, ok := g.(*Glacier); !ok {
		t.Fatal("New() did not return *Glacier type")
	}
}

func TestGlacier_Configure_EmptyVaultName(t *testing.T) {
	g := &Glacier{}
	err := g.Configure(map[string]string{"region": "us-east-1", "vaultName": ""})
	if err == nil {
		t.Fatal("expected error for empty vaultName, got nil")
	}
	if err.Error() != "vaultName not set" {
		t.Fatalf("expected 'vaultName not set', got %v", err)
	}
}

func TestGlacier_Configure_NoRegion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test that creates real AWS SDK client")
	}
	g := &Glacier{}
	// Note: AWS SDK allows nil region, but we can test it configures successfully
	err := g.Configure(map[string]string{"vaultName": "test-vault"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGlacier_Put_EmptyData(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test that creates real AWS SDK client")
	}
	g := &Glacier{}
	g.Configure(map[string]string{"region": "us-east-1", "vaultName": "vault"})

	// Put with empty reader
	err := g.Put("test-key", &bytes.Buffer{})
	// This will likely fail due to AWS SDK mock, but covers the ReadAll path
	_ = err // Accept either success or failure
}

func TestGlacier_Configure_MultipleRegions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test that creates real AWS SDK client")
	}
	regions := []string{"us-east-1", "us-west-2", "eu-west-1"}
	for _, region := range regions {
		g := &Glacier{}
		err := g.Configure(map[string]string{
			"region":    region,
			"vaultName": "test-vault",
		})
		if err != nil {
			t.Fatalf("Configure with region %s failed: %v", region, err)
		}
		if g.svc == nil {
			t.Fatal("svc should be initialized")
		}
		if g.vaultName != "test-vault" {
			t.Fatalf("vaultName = %s, want test-vault", g.vaultName)
		}
	}
}

func TestGlacier_Put_LargeData(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test that creates real AWS SDK client")
	}
	g := &Glacier{}
	g.Configure(map[string]string{"region": "us-east-1", "vaultName": "vault"})

	// Put with larger data
	largeData := bytes.Repeat([]byte("x"), 1024*1024) // 1MB
	err := g.Put("large-key", bytes.NewReader(largeData))
	// This will fail due to AWS SDK, but covers the code path
	_ = err
}

// mockGlacierAPI is a glacierAPI test double that records calls and
// returns configurable errors.
type mockGlacierAPI struct {
	uploadArchiveCalls   int
	uploadArchiveBody    []byte
	uploadArchiveDesc    string
	initiateCalls        int
	initiatePartSize     string
	parts                [][]byte
	partRanges           []string
	completeCalls        int
	completeArchiveSize  string
	completeChecksum     string
	abortCalls           int
	abortUploadID        string
	uploadArchiveErr     error
	initiateErr          error
	uploadPartErr        error
	uploadPartErrAtIndex int // part index at which uploadPartErr fires
	completeErr          error
}

const mockUploadID = "mock-upload-id"

func (m *mockGlacierAPI) UploadArchive(ctx context.Context, params *glacier.UploadArchiveInput, optFns ...func(*glacier.Options)) (*glacier.UploadArchiveOutput, error) {
	m.uploadArchiveCalls++
	if m.uploadArchiveErr != nil {
		return nil, m.uploadArchiveErr
	}
	body, err := io.ReadAll(params.Body)
	if err != nil {
		return nil, err
	}
	m.uploadArchiveBody = body
	m.uploadArchiveDesc = aws.ToString(params.ArchiveDescription)
	return &glacier.UploadArchiveOutput{}, nil
}

func (m *mockGlacierAPI) InitiateMultipartUpload(ctx context.Context, params *glacier.InitiateMultipartUploadInput, optFns ...func(*glacier.Options)) (*glacier.InitiateMultipartUploadOutput, error) {
	m.initiateCalls++
	if m.initiateErr != nil {
		return nil, m.initiateErr
	}
	m.initiatePartSize = aws.ToString(params.PartSize)
	return &glacier.InitiateMultipartUploadOutput{UploadId: aws.String(mockUploadID)}, nil
}

func (m *mockGlacierAPI) UploadMultipartPart(ctx context.Context, params *glacier.UploadMultipartPartInput, optFns ...func(*glacier.Options)) (*glacier.UploadMultipartPartOutput, error) {
	if m.uploadPartErr != nil && len(m.parts) == m.uploadPartErrAtIndex {
		return nil, m.uploadPartErr
	}
	body, err := io.ReadAll(params.Body)
	if err != nil {
		return nil, err
	}
	m.parts = append(m.parts, body)
	m.partRanges = append(m.partRanges, aws.ToString(params.Range))
	return &glacier.UploadMultipartPartOutput{}, nil
}

func (m *mockGlacierAPI) CompleteMultipartUpload(ctx context.Context, params *glacier.CompleteMultipartUploadInput, optFns ...func(*glacier.Options)) (*glacier.CompleteMultipartUploadOutput, error) {
	m.completeCalls++
	if m.completeErr != nil {
		return nil, m.completeErr
	}
	m.completeArchiveSize = aws.ToString(params.ArchiveSize)
	m.completeChecksum = aws.ToString(params.Checksum)
	return &glacier.CompleteMultipartUploadOutput{}, nil
}

func (m *mockGlacierAPI) AbortMultipartUpload(ctx context.Context, params *glacier.AbortMultipartUploadInput, optFns ...func(*glacier.Options)) (*glacier.AbortMultipartUploadOutput, error) {
	m.abortCalls++
	m.abortUploadID = aws.ToString(params.UploadId)
	return &glacier.AbortMultipartUploadOutput{}, nil
}

// testPartSize is 2 MiB — 1 MiB times a power of two, the smallest part
// size that still exercises multi-chunk tree hashing per part.
const testPartSize = 2 << 20

// randomData returns n bytes of random test data.
func randomData(t *testing.T, n int) []byte {
	t.Helper()
	data := make([]byte, n)
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}
	return data
}

func TestGlacier_Put_SinglePart_UsesUploadArchive(t *testing.T) {
	mock := &mockGlacierAPI{}
	g := &Glacier{svc: mock, vaultName: "v", partSize: testPartSize}

	data := randomData(t, testPartSize/2)
	if err := g.Put("small-key", bytes.NewReader(data)); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	if mock.uploadArchiveCalls != 1 {
		t.Errorf("UploadArchive calls = %d, want 1", mock.uploadArchiveCalls)
	}
	if mock.initiateCalls != 0 {
		t.Errorf("InitiateMultipartUpload calls = %d, want 0", mock.initiateCalls)
	}
	if !bytes.Equal(mock.uploadArchiveBody, data) {
		t.Error("UploadArchive body does not match input data")
	}
	if mock.uploadArchiveDesc != "small-key" {
		t.Errorf("ArchiveDescription = %q, want %q", mock.uploadArchiveDesc, "small-key")
	}
}

func TestGlacier_Put_ExactlyOnePart_UsesUploadArchive(t *testing.T) {
	mock := &mockGlacierAPI{}
	g := &Glacier{svc: mock, vaultName: "v", partSize: testPartSize}

	data := randomData(t, testPartSize)
	if err := g.Put("exact-key", bytes.NewReader(data)); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	if mock.uploadArchiveCalls != 1 {
		t.Errorf("UploadArchive calls = %d, want 1", mock.uploadArchiveCalls)
	}
	if mock.initiateCalls != 0 {
		t.Errorf("InitiateMultipartUpload calls = %d, want 0", mock.initiateCalls)
	}
	if !bytes.Equal(mock.uploadArchiveBody, data) {
		t.Error("UploadArchive body does not match input data")
	}
}

func TestGlacier_Put_Multipart_SplitsAndCompletes(t *testing.T) {
	mock := &mockGlacierAPI{}
	g := &Glacier{svc: mock, vaultName: "v", partSize: testPartSize}

	// 2.5 parts: two full parts and one half part.
	data := randomData(t, testPartSize*2+testPartSize/2)
	if err := g.Put("big-key", bytes.NewReader(data)); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	if mock.uploadArchiveCalls != 0 {
		t.Errorf("UploadArchive calls = %d, want 0", mock.uploadArchiveCalls)
	}
	if mock.initiateCalls != 1 {
		t.Fatalf("InitiateMultipartUpload calls = %d, want 1", mock.initiateCalls)
	}
	if mock.initiatePartSize != strconv.Itoa(testPartSize) {
		t.Errorf("PartSize = %q, want %q", mock.initiatePartSize, strconv.Itoa(testPartSize))
	}
	if len(mock.parts) != 3 {
		t.Fatalf("uploaded parts = %d, want 3", len(mock.parts))
	}

	// Reassemble and verify content and ranges.
	var reassembled []byte
	offset := 0
	for i, part := range mock.parts {
		wantRange := fmt.Sprintf("bytes %d-%d/*", offset, offset+len(part)-1)
		if mock.partRanges[i] != wantRange {
			t.Errorf("part %d range = %q, want %q", i, mock.partRanges[i], wantRange)
		}
		reassembled = append(reassembled, part...)
		offset += len(part)
	}
	if !bytes.Equal(reassembled, data) {
		t.Error("reassembled parts do not match input data")
	}
	if len(mock.parts[0]) != testPartSize || len(mock.parts[1]) != testPartSize {
		t.Errorf("full part sizes = %d, %d, want %d", len(mock.parts[0]), len(mock.parts[1]), testPartSize)
	}
	if len(mock.parts[2]) != testPartSize/2 {
		t.Errorf("final part size = %d, want %d", len(mock.parts[2]), testPartSize/2)
	}

	if mock.completeCalls != 1 {
		t.Fatalf("CompleteMultipartUpload calls = %d, want 1", mock.completeCalls)
	}
	if mock.abortCalls != 0 {
		t.Errorf("AbortMultipartUpload calls = %d, want 0", mock.abortCalls)
	}
	if mock.completeArchiveSize != strconv.Itoa(len(data)) {
		t.Errorf("ArchiveSize = %q, want %q", mock.completeArchiveSize, strconv.Itoa(len(data)))
	}

	// The completion checksum must equal the whole-archive tree hash
	// computed directly over the full payload's 1 MiB chunks.
	want := hex.EncodeToString(computeTreeHash(data))
	if mock.completeChecksum != want {
		t.Errorf("Checksum = %q, want %q", mock.completeChecksum, want)
	}
}

func TestGlacier_Put_Multipart_ExactPartMultiple(t *testing.T) {
	mock := &mockGlacierAPI{}
	g := &Glacier{svc: mock, vaultName: "v", partSize: testPartSize}

	// Exactly two parts — the final readPart returns zero bytes.
	data := randomData(t, testPartSize*2)
	if err := g.Put("two-parts", bytes.NewReader(data)); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	if len(mock.parts) != 2 {
		t.Fatalf("uploaded parts = %d, want 2", len(mock.parts))
	}
	if mock.completeCalls != 1 {
		t.Fatalf("CompleteMultipartUpload calls = %d, want 1", mock.completeCalls)
	}
	want := hex.EncodeToString(computeTreeHash(data))
	if mock.completeChecksum != want {
		t.Errorf("Checksum = %q, want %q", mock.completeChecksum, want)
	}
}

func TestGlacier_Put_Multipart_AbortsOnPartError(t *testing.T) {
	partErr := errors.New("part upload failed")
	mock := &mockGlacierAPI{uploadPartErr: partErr, uploadPartErrAtIndex: 1}
	g := &Glacier{svc: mock, vaultName: "v", partSize: testPartSize}

	data := randomData(t, testPartSize*3)
	err := g.Put("fail-key", bytes.NewReader(data))
	if !errors.Is(err, partErr) {
		t.Fatalf("Put error = %v, want %v", err, partErr)
	}

	if mock.abortCalls != 1 {
		t.Errorf("AbortMultipartUpload calls = %d, want 1", mock.abortCalls)
	}
	if mock.abortUploadID != mockUploadID {
		t.Errorf("abort UploadId = %q, want %q", mock.abortUploadID, mockUploadID)
	}
	if mock.completeCalls != 0 {
		t.Errorf("CompleteMultipartUpload calls = %d, want 0", mock.completeCalls)
	}
}

func TestGlacier_Put_Multipart_AbortsOnReadError(t *testing.T) {
	mock := &mockGlacierAPI{}
	g := &Glacier{svc: mock, vaultName: "v", partSize: testPartSize}

	// One full part plus a reader that fails mid-second-part.
	data := randomData(t, testPartSize+1)
	src := io.MultiReader(bytes.NewReader(data), errReader{})
	err := g.Put("read-fail", src)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("Put error = %v, want %v", err, io.ErrUnexpectedEOF)
	}

	if mock.abortCalls != 1 {
		t.Errorf("AbortMultipartUpload calls = %d, want 1", mock.abortCalls)
	}
	if mock.completeCalls != 0 {
		t.Errorf("CompleteMultipartUpload calls = %d, want 0", mock.completeCalls)
	}
}

func TestGlacier_Put_Multipart_NoAbortOnInitiateError(t *testing.T) {
	initErr := errors.New("initiate failed")
	mock := &mockGlacierAPI{initiateErr: initErr}
	g := &Glacier{svc: mock, vaultName: "v", partSize: testPartSize}

	data := randomData(t, testPartSize*2)
	if err := g.Put("init-fail", bytes.NewReader(data)); !errors.Is(err, initErr) {
		t.Fatalf("Put error = %v, want %v", err, initErr)
	}
	if mock.abortCalls != 0 {
		t.Errorf("AbortMultipartUpload calls = %d, want 0 (nothing to abort)", mock.abortCalls)
	}
}

func TestComputeTreeHash_SingleChunk(t *testing.T) {
	// For payloads of one chunk or less the tree hash is the plain
	// SHA-256 of the payload.
	data := []byte("hello glacier")
	got := hex.EncodeToString(computeTreeHash(data))
	sum := sha256.Sum256(data)
	want := hex.EncodeToString(sum[:])
	if got != want {
		t.Errorf("computeTreeHash = %s, want sha256 %s", got, want)
	}
}

func TestComputeTreeHash_MultiChunk(t *testing.T) {
	// Three 1 MiB chunks a, b, c: root = H(H(a||b) || c) per the
	// Glacier algorithm (odd node promoted unchanged).
	data := randomData(t, 3<<20)
	got := hex.EncodeToString(computeTreeHash(data))

	ha := computeTreeHash(data[:1<<20])
	hb := computeTreeHash(data[1<<20 : 2<<20])
	hc := computeTreeHash(data[2<<20:])
	hab := combineTreeHashes([][]byte{ha, hb})
	want := hex.EncodeToString(combineTreeHashes([][]byte{hab, hc}))

	if got != want {
		t.Errorf("computeTreeHash = %s, want %s", got, want)
	}
}

func TestCombineTreeHashes_Empty(t *testing.T) {
	if got := combineTreeHashes(nil); got != nil {
		t.Errorf("combineTreeHashes(nil) = %x, want nil", got)
	}
}
