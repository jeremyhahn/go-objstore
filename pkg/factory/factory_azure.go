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

package factory

import (
	"github.com/jeremyhahn/go-objstore/pkg/azure"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

func init() {
	RegisterStorage("azure", func(settings map[string]string, creds common.Credentials) (common.Storage, error) {
		storage := azure.New()
		err := storage.Configure(settings, creds)
		if err != nil {
			return nil, err
		}
		return storage, nil
	})
	RegisterBucketManager("azure", func(settings map[string]string, creds common.Credentials) (common.BucketManager, error) {
		manager, err := azure.NewManager(settings, creds)
		if err != nil {
			return nil, err
		}
		return manager, nil
	})
}
