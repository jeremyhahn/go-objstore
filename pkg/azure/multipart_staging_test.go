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

//go:build azureblob

package azure

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// stagedBlobs lists the blobs that hold uncommitted blocks.
func stagedBlobs(acct *svcAccount) []string {
	acct.mu.Lock()
	defer acct.mu.Unlock()
	var out []string
	for name, blocks := range acct.buckets[svcTestBucket].staged {
		if len(blocks) > 0 {
			out = append(out, name)
		}
	}
	return out
}

func readObject(t *testing.T, a *Azure, key string) string {
	t.Helper()
	r, err := a.Get(key)
	if err != nil {
		t.Fatalf("Get(%q) = %v", key, err)
	}
	defer func() { _ = r.Close() }()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// uploadOne starts an upload of key holding a single part and returns its ID
// and the part to complete it with.
func uploadOne(t *testing.T, a *Azure, key, body string) (string, common.CompletedPart) {
	t.Helper()
	ctx := context.Background()
	id, err := a.InitiateMultipartUpload(ctx, key, nil)
	if err != nil {
		t.Fatal(err)
	}
	p, err := a.UploadPart(ctx, key, id, 1, strings.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatal(err)
	}
	return id, common.CompletedPart{PartNumber: 1, ETag: p.ETag}
}

func TestAzureConcurrentUploadsOfOneKey(t *testing.T) {
	ctx := context.Background()
	a, acct := newSvcAzure(t)
	idA, partA := uploadOne(t, a, "obj", "from the first upload")
	idB, partB := uploadOne(t, a, "obj", "from the second upload")

	// The uploads stage on blobs of their own, so neither disturbs the other.
	if got := stagedBlobs(acct); len(got) != 2 {
		t.Fatalf("blobs holding staged blocks = %v, want one per upload", got)
	}
	if err := a.CompleteMultipartUpload(ctx, "obj", idB, []common.CompletedPart{partB}); err != nil {
		t.Fatalf("complete second = %v", err)
	}
	if err := a.CompleteMultipartUpload(ctx, "obj", idA, []common.CompletedPart{partA}); err != nil {
		t.Fatalf("complete first = %v", err)
	}
	if got := readObject(t, a, "obj"); got != "from the first upload" {
		t.Errorf("object = %q, want the upload that completed last", got)
	}
	if names := acct.names(svcTestBucket); len(names) != 1 || names[0] != "obj" {
		t.Errorf("blobs after both complete = %v", names)
	}
	if got := stagedBlobs(acct); len(got) != 0 {
		t.Errorf("staged blocks left on %v", got)
	}
}

func TestAzureConcurrentUploadsRace(t *testing.T) {
	ctx := context.Background()
	a, acct := newSvcAzure(t)
	const n = 6
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := a.InitiateMultipartUpload(ctx, "obj", nil)
			if err != nil {
				errs <- err
				return
			}
			p, err := a.UploadPart(ctx, "obj", id, 1, strings.NewReader("body"), 4)
			if err != nil {
				errs <- err
				return
			}
			errs <- a.CompleteMultipartUpload(ctx, "obj", id, []common.CompletedPart{{PartNumber: 1, ETag: p.ETag}})
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("upload = %v", err)
		}
	}
	if got := readObject(t, a, "obj"); got != "body" {
		t.Errorf("object = %q", got)
	}
	if names := acct.names(svcTestBucket); len(names) != 1 {
		t.Errorf("blobs after the race = %v", names)
	}
}

func TestAzureOverwriteDuringUpload(t *testing.T) {
	ctx := context.Background()
	a, acct := newSvcAzure(t)
	id, part := uploadOne(t, a, "obj", "the uploaded body")

	// A plain write commits the target key, which discards any uncommitted
	// block staged on it; the upload must not depend on that blob.
	if err := a.Put("obj", strings.NewReader("overwritten")); err != nil {
		t.Fatal(err)
	}
	if err := a.CompleteMultipartUpload(ctx, "obj", id, []common.CompletedPart{part}); err != nil {
		t.Fatalf("complete after an overwrite = %v", err)
	}
	if got := readObject(t, a, "obj"); got != "the uploaded body" {
		t.Errorf("object = %q", got)
	}
	if names := acct.names(svcTestBucket); len(names) != 1 || names[0] != "obj" {
		t.Errorf("blobs after complete = %v", names)
	}
}

func TestAzureAbortRemovesStagingBlob(t *testing.T) {
	ctx := context.Background()
	a, acct := newSvcAzure(t)
	id, _ := uploadOne(t, a, "obj", "part")
	if got := stagedBlobs(acct); len(got) != 1 || got[0] != stagingBlob(id) {
		t.Fatalf("staged blobs = %v, want %q", got, stagingBlob(id))
	}
	if !strings.HasPrefix(stagingBlob(id), common.MultipartPrefix) {
		t.Errorf("staging blob %q is outside the reserved prefix", stagingBlob(id))
	}
	if err := a.AbortMultipartUpload(ctx, "obj", id); err != nil {
		t.Fatal(err)
	}
	if names := acct.names(svcTestBucket); len(names) != 0 {
		t.Errorf("abort left %v", names)
	}
	if got := stagedBlobs(acct); len(got) != 0 {
		t.Errorf("abort left staged blocks on %v", got)
	}
}

func TestAzureAbortBeforeAnyPart(t *testing.T) {
	ctx := context.Background()
	a, acct := newSvcAzure(t)
	id, err := a.InitiateMultipartUpload(ctx, "obj", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.AbortMultipartUpload(ctx, "obj", id); err != nil {
		t.Fatal(err)
	}
	if names := acct.names(svcTestBucket); len(names) != 0 {
		t.Errorf("abort left %v", names)
	}
}

func TestAzureExpiryRemovesStagingBlob(t *testing.T) {
	ctx := context.Background()
	a, acct := newSvcAzure(t)
	a.multipartExpiry = 20 * time.Millisecond
	id, _ := uploadOne(t, a, "obj", "part")
	time.Sleep(40 * time.Millisecond)

	if _, err := a.UploadPart(ctx, "obj", id, 2, strings.NewReader("p"), 1); !errors.Is(err, common.ErrUploadNotFound) {
		t.Fatalf("upload to an expired upload = %v", err)
	}
	if names := acct.names(svcTestBucket); len(names) != 0 {
		t.Errorf("expiry left %v", names)
	}
	if got := stagedBlobs(acct); len(got) != 0 {
		t.Errorf("expiry left staged blocks on %v", got)
	}
}

func TestAzureCompleteWithoutBlockSources(t *testing.T) {
	ctx := context.Background()
	a, acct := newSvcAzure(t)
	id, part := uploadOne(t, a, "obj", "part")
	a.container = noCopyContainer{a.container}
	if err := a.CompleteMultipartUpload(ctx, "obj", id, []common.CompletedPart{part}); !errors.Is(err, common.ErrNotSupported) {
		t.Errorf("complete without a source for staged blocks = %v", err)
	}
	if names := acct.names(svcTestBucket); len(names) == 0 {
		t.Error("a failed complete must leave the upload for a retry or an abort")
	}
}

// smallPublishBlocks makes a publication take several blocks of a short body.
func smallPublishBlocks(t *testing.T, size int64) {
	t.Helper()
	old := publishBlockSize
	publishBlockSize = size
	t.Cleanup(func() { publishBlockSize = old })
}

func visible(acct *svcAccount, key string) bool {
	for _, n := range acct.names(svcTestBucket) {
		if n == key {
			return true
		}
	}
	return false
}

func TestAzurePublishesInBlocks(t *testing.T) {
	ctx := context.Background()
	smallPublishBlocks(t, 4)
	a, acct := newSvcAzure(t)
	id, part := uploadOne(t, a, "obj", "0123456789")
	if err := a.CompleteMultipartUpload(ctx, "obj", id, []common.CompletedPart{part}); err != nil {
		t.Fatal(err)
	}
	if got := readObject(t, a, "obj"); got != "0123456789" {
		t.Errorf("object = %q", got)
	}
	acct.mu.Lock()
	blocks := len(acct.buckets[svcTestBucket].objects["obj"].blocks)
	acct.mu.Unlock()
	if blocks != 3 {
		t.Errorf("target blocks = %d, want 3", blocks)
	}
}

func TestAzurePublishFailureKeepsOldContent(t *testing.T) {
	ctx := context.Background()
	smallPublishBlocks(t, 4)
	a, acct := newSvcAzure(t)
	if err := a.Put("obj", strings.NewReader("old")); err != nil {
		t.Fatal(err)
	}
	id, part := uploadOne(t, a, "obj", "0123456789")

	errBoom := errors.New("boom")
	staged := 0
	acct.fail = func(op string) error {
		if op == "stageurl" {
			staged++
			if staged == 2 {
				return errBoom
			}
		}
		return nil
	}
	if err := a.CompleteMultipartUpload(ctx, "obj", id, []common.CompletedPart{part}); !errors.Is(err, errBoom) {
		t.Fatalf("complete = %v, want the staging failure", err)
	}
	if got := readObject(t, a, "obj"); got != "old" {
		t.Errorf("object after a failed publication = %q, want the old content", got)
	}

	// The retry publishes again; it does not ask for the parts again.
	if err := a.CompleteMultipartUpload(ctx, "obj", id, []common.CompletedPart{part}); err != nil {
		t.Fatalf("retry = %v", err)
	}
	if got := readObject(t, a, "obj"); got != "0123456789" {
		t.Errorf("object after the retry = %q", got)
	}
	if names := acct.names(svcTestBucket); len(names) != 1 || names[0] != "obj" {
		t.Errorf("blobs after the retry = %v", names)
	}
}

func TestAzureFailedFinalCommitIsRetryable(t *testing.T) {
	ctx := context.Background()
	a, acct := newSvcAzure(t)
	id, part := uploadOne(t, a, "obj", "the uploaded body")

	errBoom := errors.New("boom")
	commits := 0
	acct.fail = func(op string) error {
		if op == "commit" {
			commits++
			if commits == 2 { // the first commit is the staging blob's
				return errBoom
			}
		}
		return nil
	}
	if err := a.CompleteMultipartUpload(ctx, "obj", id, []common.CompletedPart{part}); !errors.Is(err, errBoom) {
		t.Fatalf("complete = %v, want the commit failure", err)
	}
	if visible(acct, "obj") {
		t.Error("a failed publication left the key visible")
	}
	if err := a.CompleteMultipartUpload(ctx, "obj", id, []common.CompletedPart{part}); err != nil {
		t.Fatalf("retry = %v", err)
	}
	if commits != 3 {
		t.Errorf("commits = %d, want the staging commit, the failed one and the retry", commits)
	}
	if got := readObject(t, a, "obj"); got != "the uploaded body" {
		t.Errorf("object = %q", got)
	}
}

func TestAzurePublishRestagesDiscardedBlocks(t *testing.T) {
	ctx := context.Background()
	a, acct := newSvcAzure(t)
	id, part := uploadOne(t, a, "obj", "the uploaded body")
	commits := 0
	acct.fail = func(op string) error {
		if op == "commit" {
			commits++
			if commits == 2 { // another upload's commit discarded the blocks
				return &svcError{azblob.ServiceCodeInvalidBlockList}
			}
		}
		return nil
	}
	if err := a.CompleteMultipartUpload(ctx, "obj", id, []common.CompletedPart{part}); err != nil {
		t.Fatalf("complete = %v", err)
	}
	if got := readObject(t, a, "obj"); got != "the uploaded body" {
		t.Errorf("object = %q", got)
	}
}

func TestAzureRetryChecksTheCommittedParts(t *testing.T) {
	ctx := context.Background()
	a, acct := newSvcAzure(t)
	id, part := uploadOne(t, a, "obj", "the uploaded body")
	acct.fail = func(op string) error {
		if op == "stageurl" {
			return errors.New("boom")
		}
		return nil
	}
	if err := a.CompleteMultipartUpload(ctx, "obj", id, []common.CompletedPart{part}); err == nil {
		t.Fatal("complete succeeded")
	}
	acct.fail = nil

	wrong := common.CompletedPart{PartNumber: part.PartNumber, ETag: "not-the-etag"}
	if err := a.CompleteMultipartUpload(ctx, "obj", id, []common.CompletedPart{wrong}); !errors.Is(err, common.ErrPartMismatch) {
		t.Errorf("retry with a wrong ETag = %v", err)
	}
	other := common.CompletedPart{PartNumber: 2, ETag: part.ETag}
	if err := a.CompleteMultipartUpload(ctx, "obj", id, []common.CompletedPart{other}); !errors.Is(err, common.ErrPartMismatch) {
		t.Errorf("retry with a part that was not uploaded = %v", err)
	}
	if visible(acct, "obj") {
		t.Error("a rejected retry published the key")
	}
}

func TestAzureAbortAfterFailedPublication(t *testing.T) {
	ctx := context.Background()
	a, acct := newSvcAzure(t)
	id, part := uploadOne(t, a, "obj", "part")
	acct.fail = func(op string) error {
		if op == "stageurl" {
			return errors.New("boom")
		}
		return nil
	}
	if err := a.CompleteMultipartUpload(ctx, "obj", id, []common.CompletedPart{part}); err == nil {
		t.Fatal("complete succeeded")
	}
	acct.fail = nil
	if err := a.AbortMultipartUpload(ctx, "obj", id); err != nil {
		t.Fatal(err)
	}
	if names := acct.names(svcTestBucket); len(names) != 0 {
		t.Errorf("abort left %v, the committed staging blob among them", names)
	}
}

func TestAzureExpiryAfterFailedPublication(t *testing.T) {
	ctx := context.Background()
	a, acct := newSvcAzure(t)
	a.multipartExpiry = time.Hour
	id, part := uploadOne(t, a, "obj", "part")
	acct.fail = func(op string) error {
		if op == "stageurl" {
			return errors.New("boom")
		}
		return nil
	}
	if err := a.CompleteMultipartUpload(ctx, "obj", id, []common.CompletedPart{part}); err == nil {
		t.Fatal("complete succeeded")
	}
	acct.fail = nil
	a.multipartExpiry = time.Nanosecond
	time.Sleep(time.Millisecond)
	if err := a.CompleteMultipartUpload(ctx, "obj", id, []common.CompletedPart{part}); !errors.Is(err, common.ErrUploadNotFound) {
		t.Fatalf("complete after expiry = %v", err)
	}
	if names := acct.names(svcTestBucket); len(names) != 0 {
		t.Errorf("expiry left %v", names)
	}
}

func TestAzurePublishNeedsTooManyBlocks(t *testing.T) {
	ctx := context.Background()
	smallPublishBlocks(t, 1)
	a, acct := newSvcAzure(t)
	id, part := uploadOne(t, a, "obj", strings.Repeat("x", maxBlocks+1))
	if err := a.CompleteMultipartUpload(ctx, "obj", id, []common.CompletedPart{part}); !errors.Is(err, common.ErrPartTooLarge) {
		t.Errorf("complete = %v", err)
	}
	if visible(acct, "obj") {
		t.Error("a refused publication left the key visible")
	}
}

func TestAzureSourceURLIsSigned(t *testing.T) {
	cred, err := azblob.NewSharedKeyCredential("account", "a2V5MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTA=")
	if err != nil {
		t.Fatal(err)
	}
	a, acct := newSvcAzure(t)
	src := acct.Container(svcTestBucket).(sourceAPI)

	unsigned, err := a.sourceURL(src, "staging")
	if err != nil || unsigned.RawQuery != "" {
		t.Errorf("without a key the URL = %q, %v", unsigned.String(), err)
	}

	a.sharedKey = cred
	u, err := a.sourceURL(src, "staging")
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	if q.Get("sp") != "r" || q.Get("sr") != "b" || q.Get("sig") == "" || q.Get("spr") != "https" {
		t.Errorf("signature is not a read-only HTTPS signature on one blob: %q", u.RawQuery)
	}
	expiry, err := time.Parse(time.RFC3339, q.Get("se"))
	if err != nil || time.Until(expiry) > 2*sourceLifetime || time.Until(expiry) <= 0 {
		t.Errorf("signature expiry %q is not short-lived", q.Get("se"))
	}
	if u.Path != "/"+svcTestBucket+"/staging" {
		t.Errorf("path = %q", u.Path)
	}
}

// noCopyContainer hides the server-side copy of the wrapped container.
type noCopyContainer struct{ ContainerAPI }
