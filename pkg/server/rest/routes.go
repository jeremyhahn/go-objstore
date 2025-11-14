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

package rest

import (
	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
)

// SetupRoutes configures all routes for the REST API
func SetupRoutes(router *gin.Engine, handler *Handler) {
	// Health check endpoint (no auth required)
	router.GET("/health", handler.HealthCheck)

	// Swagger documentation
	router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	// API v1 group
	v1 := router.Group("/api/v1")
	{
		// Metadata operations (use wildcard to support keys with slashes)
		v1.GET("/metadata/*key", handler.GetObjectMetadata)
		v1.PUT("/metadata/*key", handler.UpdateObjectMetadata)

		// Exists check (must be before /objects/*key to avoid route conflict)
		v1.HEAD("/exists/*key", handler.ExistsObject)

		// Object operations
		objects := v1.Group("/objects")
		{
			// List objects
			objects.GET("", handler.ListObjects)

			// Object CRUD operations
			objects.PUT("/*key", handler.PutObject)
			objects.GET("/*key", handler.GetObject)
			objects.DELETE("/*key", handler.DeleteObject)
			objects.HEAD("/*key", handler.HeadObject)
		}

		// Archive operations
		v1.POST("/archive", handler.Archive)

		// Lifecycle policy operations
		policies := v1.Group("/policies")
		{
			policies.GET("", handler.GetPolicies)
			policies.POST("", handler.AddPolicy)
			policies.DELETE("/*id", handler.RemovePolicy)
			policies.POST("/apply", handler.ApplyPolicies)
		}

		// Replication policy operations
		replication := v1.Group("/replication")
		{
			replication.POST("/policies", handler.AddReplicationPolicy)
			replication.GET("/policies", handler.GetReplicationPolicies)
			replication.GET("/policies/*id", handler.GetReplicationPolicy)
			replication.DELETE("/policies/*id", handler.RemoveReplicationPolicy)
			replication.POST("/trigger", handler.TriggerReplication)
			replication.GET("/status/*id", handler.GetReplicationStatus)
		}
	}

	// Backwards compatibility: support routes without /api/v1 prefix
	router.GET("/metadata/*key", handler.GetObjectMetadata)
	router.PUT("/metadata/*key", handler.UpdateObjectMetadata)
	router.HEAD("/exists/*key", handler.ExistsObject)
	router.GET("/objects", handler.ListObjects)
	router.PUT("/objects/*key", handler.PutObject)
	router.GET("/objects/*key", handler.GetObject)
	router.DELETE("/objects/*key", handler.DeleteObject)
	router.HEAD("/objects/*key", handler.HeadObject)

	// Archive and policy routes (backwards compatibility)
	router.POST("/archive", handler.Archive)
	router.GET("/policies", handler.GetPolicies)
	router.POST("/policies", handler.AddPolicy)
	router.DELETE("/policies/*id", handler.RemovePolicy)
	router.POST("/policies/apply", handler.ApplyPolicies)

	// Replication routes (backwards compatibility)
	router.POST("/replication/policies", handler.AddReplicationPolicy)
	router.GET("/replication/policies", handler.GetReplicationPolicies)
	router.GET("/replication/policies/*id", handler.GetReplicationPolicy)
	router.DELETE("/replication/policies/*id", handler.RemoveReplicationPolicy)
	router.POST("/replication/trigger", handler.TriggerReplication)
	router.GET("/replication/status/*id", handler.GetReplicationStatus)
}
