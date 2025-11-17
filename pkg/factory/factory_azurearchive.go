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

//go:build azurearchive

package factory

import (
	"github.com/jeremyhahn/go-objstore/pkg/azurearchive"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

func init() {
	RegisterArchiver("azurearchive", func(settings map[string]string) (common.Archiver, error) {
		archiver := azurearchive.New()
		azureArchiver, ok := archiver.(*azurearchive.AzureArchive)
		if !ok {
			return nil, ErrTypeAssertionFailed
		}
		err := azureArchiver.Configure(settings)
		if err != nil {
			return nil, err
		}
		return archiver, nil
	})
}
