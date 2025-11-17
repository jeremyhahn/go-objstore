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

package middleware

import (
	"fmt"

	"github.com/gin-gonic/gin"
)

// SecurityHeadersConfig holds security headers configuration
type SecurityHeadersConfig struct {
	// EnableHSTS enables HTTP Strict Transport Security
	EnableHSTS bool

	// HSTSMaxAge is the max-age for HSTS header (default: 31536000 = 1 year)
	HSTSMaxAge int

	// HSTSIncludeSubdomains includes subdomains in HSTS
	HSTSIncludeSubdomains bool

	// HSTSPreload enables HSTS preload
	HSTSPreload bool

	// ContentSecurityPolicy sets the CSP header (default: "default-src 'self'")
	ContentSecurityPolicy string

	// XFrameOptions sets the X-Frame-Options header (default: "DENY")
	XFrameOptions string

	// XContentTypeOptions sets the X-Content-Type-Options header (default: "nosniff")
	XContentTypeOptions string

	// XXSSProtection sets the X-XSS-Protection header (default: "1; mode=block")
	XXSSProtection string

	// ReferrerPolicy sets the Referrer-Policy header (default: "strict-origin-when-cross-origin")
	ReferrerPolicy string

	// PermissionsPolicy sets the Permissions-Policy header
	PermissionsPolicy string
}

// DefaultSecurityHeadersConfig returns security headers config with sensible defaults
func DefaultSecurityHeadersConfig() *SecurityHeadersConfig {
	return &SecurityHeadersConfig{
		EnableHSTS:            false, // Only enable when using TLS
		HSTSMaxAge:            31536000,
		HSTSIncludeSubdomains: true,
		HSTSPreload:           false,
		ContentSecurityPolicy: "default-src 'self'",
		XFrameOptions:         "DENY",
		XContentTypeOptions:   "nosniff",
		XXSSProtection:        "1; mode=block",
		ReferrerPolicy:        "strict-origin-when-cross-origin",
		PermissionsPolicy:     "",
	}
}

// SecurityHeadersMiddleware creates a Gin middleware that sets security headers
func SecurityHeadersMiddleware(config *SecurityHeadersConfig) gin.HandlerFunc {
	if config == nil {
		config = DefaultSecurityHeadersConfig()
	}

	return func(c *gin.Context) {
		// X-Content-Type-Options: Prevents MIME type sniffing
		if config.XContentTypeOptions != "" {
			c.Header("X-Content-Type-Options", config.XContentTypeOptions)
		}

		// X-Frame-Options: Prevents clickjacking attacks
		if config.XFrameOptions != "" {
			c.Header("X-Frame-Options", config.XFrameOptions)
		}

		// X-XSS-Protection: Enables browser's XSS protection
		if config.XXSSProtection != "" {
			c.Header("X-XSS-Protection", config.XXSSProtection)
		}

		// Content-Security-Policy: Controls resource loading
		if config.ContentSecurityPolicy != "" {
			c.Header("Content-Security-Policy", config.ContentSecurityPolicy)
		}

		// Referrer-Policy: Controls referrer information
		if config.ReferrerPolicy != "" {
			c.Header("Referrer-Policy", config.ReferrerPolicy)
		}

		// Permissions-Policy: Controls browser features
		if config.PermissionsPolicy != "" {
			c.Header("Permissions-Policy", config.PermissionsPolicy)
		}

		// Strict-Transport-Security: Only set if TLS is enabled
		if config.EnableHSTS && c.Request.TLS != nil {
			hstsValue := formatHSTSHeader(config)
			c.Header("Strict-Transport-Security", hstsValue)
		}

		c.Next()
	}
}

// formatHSTSHeader formats the HSTS header value
func formatHSTSHeader(config *SecurityHeadersConfig) string {
	hstsValue := ""

	if config.HSTSMaxAge > 0 {
		hstsValue = fmt.Sprintf("max-age=%d", config.HSTSMaxAge)
	}

	if config.HSTSIncludeSubdomains {
		if hstsValue != "" {
			hstsValue += "; "
		}
		hstsValue += "includeSubDomains"
	}

	if config.HSTSPreload {
		if hstsValue != "" {
			hstsValue += "; "
		}
		hstsValue += "preload"
	}

	return hstsValue
}
