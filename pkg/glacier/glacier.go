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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/jeremyhahn/go-objstore/pkg/common"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/glacier"
)

const (
	// defaultPartSize is the multipart upload part size. Glacier requires
	// part sizes of 1 MiB multiplied by a power of two; 16 MiB bounds
	// Put's memory usage to a single part buffer regardless of archive
	// size.
	defaultPartSize = 16 << 20

	// treeHashChunkSize is the 1 MiB chunk size mandated by the Glacier
	// SHA-256 tree hash algorithm. See
	// https://docs.aws.amazon.com/amazonglacier/latest/dev/checksum-calculations.html
	treeHashChunkSize = 1 << 20
)

// glacierAPI is the subset of the AWS SDK v2 Glacier client used by this
// backend. *glacier.Client satisfies it; tests substitute a mock.
type glacierAPI interface {
	UploadArchive(ctx context.Context, params *glacier.UploadArchiveInput, optFns ...func(*glacier.Options)) (*glacier.UploadArchiveOutput, error)
	InitiateMultipartUpload(ctx context.Context, params *glacier.InitiateMultipartUploadInput, optFns ...func(*glacier.Options)) (*glacier.InitiateMultipartUploadOutput, error)
	UploadMultipartPart(ctx context.Context, params *glacier.UploadMultipartPartInput, optFns ...func(*glacier.Options)) (*glacier.UploadMultipartPartOutput, error)
	CompleteMultipartUpload(ctx context.Context, params *glacier.CompleteMultipartUploadInput, optFns ...func(*glacier.Options)) (*glacier.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(ctx context.Context, params *glacier.AbortMultipartUploadInput, optFns ...func(*glacier.Options)) (*glacier.AbortMultipartUploadOutput, error)
}

// Glacier is an archive-only storage backend for AWS Glacier.
type Glacier struct {
	svc       glacierAPI
	vaultName string

	// partSize is the multipart part size in bytes. Zero means
	// defaultPartSize. It exists so tests can exercise the multipart
	// path with small payloads; it must be 1 MiB times a power of two.
	partSize int
}

// New creates a new Glacier storage backend.
func New() common.ArchiveOnlyStorage {
	return &Glacier{}
}

// Configure sets up the backend with the necessary settings.
func (g *Glacier) Configure(settings map[string]string) error {
	g.vaultName = settings["vaultName"]
	if g.vaultName == "" {
		return common.ErrVaultNotSet
	}

	ctx := context.TODO()
	var opts []func(*config.LoadOptions) error

	if region := settings["region"]; region != "" {
		opts = append(opts, config.WithRegion(region))
	}

	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return err
	}

	g.svc = glacier.NewFromConfig(cfg)
	return nil
}

// Put stores an object in the archive. Streams that fit within a single
// part are uploaded with one UploadArchive call; larger streams use the
// Glacier multipart upload API so memory usage stays bounded by a single
// part buffer instead of the whole archive.
func (g *Glacier) Put(key string, data io.Reader) error {
	// The common.Archiver interface carries no context; one TODO context
	// is shared by every SDK call this method makes.
	ctx := context.TODO()

	partSize := g.partSize
	if partSize <= 0 {
		partSize = defaultPartSize
	}

	first := make([]byte, partSize)
	n, err := readPart(data, first)
	if err != nil {
		return err
	}
	first = first[:n]

	if n == partSize {
		// The first part filled completely; peek one byte to learn
		// whether the stream has more data before committing to a
		// multipart upload.
		peek := make([]byte, 1)
		pn, err := readPart(data, peek)
		if err != nil {
			return err
		}
		if pn > 0 {
			rest := io.MultiReader(bytes.NewReader(peek[:pn]), data)
			return g.putMultipart(ctx, key, partSize, first, rest)
		}
	}

	// The whole stream fits in one part — single-shot upload. The SDK's
	// tree hash middleware computes the required checksum headers from
	// the seekable body.
	_, err = g.svc.UploadArchive(ctx, &glacier.UploadArchiveInput{
		VaultName:          aws.String(g.vaultName),
		ArchiveDescription: aws.String(key),
		Body:               bytes.NewReader(first),
	})
	return err
}

// putMultipart streams the archive to Glacier with the multipart upload
// API, buffering one part at a time. firstPart is the already-read first
// part (always exactly partSize bytes); rest supplies the remainder of
// the stream. The upload is aborted on any failure so Glacier does not
// accumulate orphaned multipart uploads.
func (g *Glacier) putMultipart(ctx context.Context, key string, partSize int, firstPart []byte, rest io.Reader) (err error) {
	initOut, err := g.svc.InitiateMultipartUpload(ctx, &glacier.InitiateMultipartUploadInput{
		VaultName:          aws.String(g.vaultName),
		ArchiveDescription: aws.String(key),
		PartSize:           aws.String(strconv.Itoa(partSize)),
	})
	if err != nil {
		return err
	}
	uploadID := initOut.UploadId

	defer func() {
		if err != nil {
			// Clean up so Glacier does not accumulate orphaned uploads;
			// the original error stays first in the joined chain.
			if _, abortErr := g.svc.AbortMultipartUpload(ctx, &glacier.AbortMultipartUploadInput{
				VaultName: aws.String(g.vaultName),
				UploadId:  uploadID,
			}); abortErr != nil {
				err = errors.Join(err, fmt.Errorf("abort multipart upload: %w", abortErr))
			}
		}
	}()

	var (
		partHashes [][]byte
		offset     int64
	)
	buf := firstPart
	for {
		// The per-part tree hash is computed here for the final
		// whole-archive checksum; the SDK's tree hash middleware
		// independently sets the per-request checksum headers from the
		// seekable part body.
		hash := computeTreeHash(buf)
		end := offset + int64(len(buf)) - 1
		if _, err = g.svc.UploadMultipartPart(ctx, &glacier.UploadMultipartPartInput{
			VaultName: aws.String(g.vaultName),
			UploadId:  uploadID,
			Body:      bytes.NewReader(buf),
			Range:     aws.String(fmt.Sprintf("bytes %d-%d/*", offset, end)),
		}); err != nil {
			return err
		}
		partHashes = append(partHashes, hash)
		offset = end + 1

		if len(buf) < partSize {
			// A short part means the stream is exhausted.
			break
		}

		buf = buf[:partSize]
		var n int
		if n, err = readPart(rest, buf); err != nil {
			return err
		}
		if n == 0 {
			break
		}
		buf = buf[:n]
	}

	// Because partSize is a power-of-two multiple of 1 MiB, combining the
	// per-part tree hash roots yields the same root as a tree built from
	// the archive's 1 MiB chunks — the value Glacier verifies on
	// completion.
	_, err = g.svc.CompleteMultipartUpload(ctx, &glacier.CompleteMultipartUploadInput{
		VaultName:   aws.String(g.vaultName),
		UploadId:    uploadID,
		ArchiveSize: aws.String(strconv.FormatInt(offset, 10)),
		Checksum:    aws.String(hex.EncodeToString(combineTreeHashes(partHashes))),
	})
	return err
}

// readPart fills buf from r, treating io.EOF as a short (possibly
// empty) read rather than an error. Unlike io.ReadFull it propagates
// io.ErrUnexpectedEOF from the underlying reader as a genuine failure.
// It returns the number of bytes read.
func readPart(r io.Reader, buf []byte) (int, error) {
	n := 0
	for n < len(buf) {
		m, err := r.Read(buf[n:])
		n += m
		if err == io.EOF {
			break
		}
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// computeTreeHash returns the SHA-256 tree hash of p per the Glacier
// checksum specification: SHA-256 over each 1 MiB chunk, then pairwise
// reduction to a single root.
func computeTreeHash(p []byte) []byte {
	var leaves [][]byte
	for len(p) > treeHashChunkSize {
		h := sha256.Sum256(p[:treeHashChunkSize])
		leaves = append(leaves, h[:])
		p = p[treeHashChunkSize:]
	}
	h := sha256.Sum256(p)
	leaves = append(leaves, h[:])
	return combineTreeHashes(leaves)
}

// combineTreeHashes reduces a level of tree hash nodes to the root by
// hashing adjacent pairs; an unpaired trailing node is promoted to the
// next level unchanged, per the Glacier tree hash algorithm.
func combineTreeHashes(hashes [][]byte) []byte {
	if len(hashes) == 0 {
		return nil
	}
	for len(hashes) > 1 {
		next := make([][]byte, 0, (len(hashes)+1)/2)
		for i := 0; i+1 < len(hashes); i += 2 {
			pair := sha256.New()
			pair.Write(hashes[i])
			pair.Write(hashes[i+1])
			next = append(next, pair.Sum(nil))
		}
		if len(hashes)%2 == 1 {
			next = append(next, hashes[len(hashes)-1])
		}
		hashes = next
	}
	return hashes[0]
}
