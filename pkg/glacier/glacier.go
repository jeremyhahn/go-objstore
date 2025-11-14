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
	"io"
	"io/ioutil"

	"github.com/jeremyhahn/go-objstore/pkg/common"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/glacier"
)

// Glacier is an archive-only storage backend for AWS Glacier.
type Glacier struct {
	svc       *glacier.Glacier
	vaultName string
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

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(settings["region"]),
	})
	if err != nil {
		return err
	}

	g.svc = glacier.New(sess)
	return nil
}

// Put stores an object in the archive.
func (g *Glacier) Put(key string, data io.Reader) error {
	// Glacier requires the content length to be known beforehand.
	// For simplicity, we'll read the entire content into a buffer.
	buf, err := ioutil.ReadAll(data)
	if err != nil {
		return err
	}

	_, err = g.svc.UploadArchive(&glacier.UploadArchiveInput{
		VaultName:          aws.String(g.vaultName),
		ArchiveDescription: aws.String(key),
		Body:               bytes.NewReader(buf),
	})
	return err
}
