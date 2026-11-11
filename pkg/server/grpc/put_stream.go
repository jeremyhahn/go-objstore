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

package grpc

import (
	"errors"
	"io"

	objstorepb "github.com/jeremyhahn/go-objstore/api/proto"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/auditlog"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/objstore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// errPutStreamAborted is the reader error the storage backend sees when the
// handler stops the put before the client finished the stream.
var errPutStreamAborted = errors.New("put stream aborted")

// PutStream stores an object sent as a client stream. The first message
// carries the key and optional metadata; every later message carries a chunk
// of object data. The chunks are fed to the storage backend through an
// io.Pipe, so the handler holds at most one chunk in memory regardless of the
// object size. A stream that fails or is cancelled before the client closes
// it makes the backend's read fail, so the backend stores nothing.
func (s *Server) PutStream(stream objstorepb.ObjectStore_PutStreamServer) error {
	ctx := stream.Context()

	first, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		return status.Error(codes.InvalidArgument, "key is required: stream ended before the header")
	}
	if err != nil {
		return err
	}
	header := first.GetHeader()
	if header == nil {
		return status.Error(codes.InvalidArgument, "key is required: the first message must carry the header")
	}
	if header.GetKey() == "" {
		return status.Error(codes.InvalidArgument, "key is required")
	}
	key := header.GetKey()

	var metadata *common.Metadata
	if header.GetMetadata() != nil {
		metadata = protoToMetadata(header.GetMetadata())
	}

	pr, pw := io.Pipe()

	// recvDone receives the receive loop's outcome (nil on a clean EOF)
	// before the pipe is closed, so when the backend's read fails because of
	// the stream, the cause is already available below.
	recvDone := make(chan error, 1)
	var received int64
	go func() {
		for {
			msg, rerr := stream.Recv()
			if errors.Is(rerr, io.EOF) {
				recvDone <- nil
				_ = pw.Close()
				return
			}
			if rerr != nil {
				recvDone <- rerr
				_ = pw.CloseWithError(rerr)
				return
			}
			if msg.GetHeader() != nil {
				herr := status.Error(codes.InvalidArgument, "header may only be sent in the first message")
				recvDone <- herr
				_ = pw.CloseWithError(herr)
				return
			}
			chunk := msg.GetChunk()
			if len(chunk) == 0 {
				continue
			}
			if _, werr := pw.Write(chunk); werr != nil {
				// The backend stopped reading; its error is reported below.
				recvDone <- nil
				return
			}
			received += int64(len(chunk))
		}
	}()

	if metadata != nil {
		err = objstore.PutWithMetadata(ctx, s.keyRef(key), pr, metadata)
	} else {
		err = objstore.PutWithContext(ctx, s.keyRef(key), pr)
	}
	// Unblock the receive loop if the backend returned before reading to EOF.
	_ = pr.CloseWithError(errPutStreamAborted)

	auditLogger := audit.GetAuditLogger(ctx)
	principal, userID := extractGRPCPrincipal(ctx)
	requestID := audit.GetRequestID(ctx)
	ipAddress := extractGRPCClientIP(ctx)

	if err != nil {
		_ = auditLogger.LogObjectMutation(ctx, auditlog.EventObjectCreated,
			userID, principal, s.backend, key, ipAddress, requestID, 0,
			auditlog.ResultFailure, err)
		// Prefer the stream's own failure (a cancel, a protocol error) over
		// the backend's wrapped read error.
		select {
		case rerr := <-recvDone:
			if rerr != nil {
				if _, ok := status.FromError(rerr); ok {
					return rerr
				}
				return status.FromContextError(rerr).Err()
			}
		default:
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return status.FromContextError(ctxErr).Err()
		}
		return mapError(err)
	}

	// The backend read to EOF, so the receive loop has finished.
	if rerr := <-recvDone; rerr != nil {
		return rerr
	}

	_ = auditLogger.LogObjectMutation(ctx, auditlog.EventObjectCreated,
		userID, principal, s.backend, key, ipAddress, requestID, received,
		auditlog.ResultSuccess, nil)

	etag := ""
	if metadata != nil {
		etag = metadata.ETag
	}

	return stream.SendAndClose(&objstorepb.PutResponse{
		Success: true,
		Message: "Object stored successfully",
		Etag:    etag,
	})
}
