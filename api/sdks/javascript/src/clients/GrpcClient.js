import grpc from '@grpc/grpc-js';
import protoLoader from '@grpc/proto-loader';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

/**
 * gRPC client for go-objstore
 * Supports all object storage operations via gRPC protocol
 */
export class GrpcClient {
  /**
   * Create a new gRPC client
   * @param {Object} config - Client configuration
   * @param {string} config.address - gRPC server address (e.g., 'localhost:50051')
   * @param {boolean} [config.insecure=true] - Use insecure connection
   * @param {Object} [config.credentials] - gRPC credentials
   */
  constructor(config) {
    if (!config?.address) {
      throw new Error('address is required');
    }

    this.address = config.address;
    this.insecure = config.insecure !== false;
    this.credentials = config.credentials;

    // Lazy initialization - client will be created on first use
    this.client = null;
    this.protoLoaded = false;
    this.ObjectStoreService = null;
  }

  /**
   * Initialize the gRPC client connection
   * This is called lazily on first API call
   * @private
   */
  _ensureClient() {
    if (this.client) {
      return;
    }

    // Load proto file on first use
    if (!this.protoLoaded) {
      const PROTO_PATH = join(__dirname, '../proto/objstore.proto');
      const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
        keepCase: true,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true,
      });

      const protoDescriptor = grpc.loadPackageDefinition(packageDefinition);
      this.ObjectStoreService = protoDescriptor.objstore.v1.ObjectStore;
      this.protoLoaded = true;
    }

    // Create client connection
    const credentials = this.credentials ||
      (this.insecure ? grpc.credentials.createInsecure() : grpc.credentials.createSsl());

    this.client = new this.ObjectStoreService(this.address, credentials);
  }

  /**
   * Store an object in the backend
   * @param {string} key - Storage key for the object
   * @param {Buffer|Uint8Array|string} data - Object data
   * @param {Object} [metadata] - Optional metadata
   * @returns {Promise<Object>} Response with success status and etag
   */
  async put(key, data, metadata = null) {
    return new Promise((resolve, reject) => {
      // Validate inputs
      if (!key) {
        reject(new Error('key is required'));
        return;
      }
      if (data === null || data === undefined) {
        reject(new Error('data is required'));
        return;
      }

      // Ensure client is initialized
      this._ensureClient();

      const request = {
        key,
        data: Buffer.from(data),
        metadata: metadata ? this._convertMetadata(metadata) : null,
      };

      this.client.Put(request, (error, response) => {
        if (error) {
          reject(this._convertError(error));
          return;
        }

        resolve({
          success: response.success,
          message: response.message,
          etag: response.etag,
        });
      });
    });
  }

  /**
   * Retrieve an object from the backend
   * @param {string} key - Storage key for the object
   * @returns {Promise<Object>} Response with data and metadata
   */
  async get(key) {
    return new Promise((resolve, reject) => {
      if (!key) {
        reject(new Error('key is required'));
        return;
      }

      // Ensure client is initialized
      this._ensureClient();

      const chunks = [];
      let metadata = null;

      const call = this.client.Get({ key });

      call.on('data', (response) => {
        if (response.metadata && !metadata) {
          metadata = this._parseMetadata(response.metadata);
        }
        if (response.data) {
          chunks.push(response.data);
        }
      });

      call.on('end', () => {
        resolve({
          data: Buffer.concat(chunks),
          metadata: metadata || {},
        });
      });

      call.on('error', (error) => {
        reject(this._convertError(error));
      });
    });
  }

  /**
   * Delete an object from the backend
   * @param {string} key - Storage key for the object
   * @returns {Promise<Object>} Response with success status
   */
  async delete(key) {
    return new Promise((resolve, reject) => {
      if (!key) {
        reject(new Error('key is required'));
        return;
      }

      // Ensure client is initialized
      this._ensureClient();

      this.client.Delete({ key }, (error, response) => {
        if (error) {
          reject(this._convertError(error));
          return;
        }

        resolve({
          success: response.success,
          message: response.message,
        });
      });
    });
  }

  /**
   * List objects matching the criteria
   * @param {Object} [options] - List options
   * @param {string} [options.prefix] - Prefix to filter objects
   * @param {string} [options.delimiter] - Delimiter for hierarchical listing
   * @param {number} [options.maxResults] - Maximum number of results
   * @param {string} [options.continueFrom] - Pagination token
   * @returns {Promise<Object>} Response with objects list
   */
  async list(options = {}) {
    return new Promise((resolve, reject) => {
      // Ensure client is initialized
      this._ensureClient();

      const request = {
        prefix: options.prefix || '',
        delimiter: options.delimiter || '',
        max_results: options.maxResults || 0,
        continue_from: options.continueFrom || '',
      };

      this.client.List(request, (error, response) => {
        if (error) {
          reject(this._convertError(error));
          return;
        }

        resolve({
          objects: response.objects || [],
          commonPrefixes: response.common_prefixes || [],
          nextToken: response.next_token || '',
          truncated: response.truncated || false,
        });
      });
    });
  }

  /**
   * List objects with advanced options
   * Provides explicit control over all listing parameters
   * @param {Object} [options] - List options
   * @param {string} [options.prefix=''] - Prefix to filter objects
   * @param {string} [options.delimiter=''] - Delimiter for hierarchical listing
   * @param {number} [options.maxResults=0] - Maximum number of results (0 = unlimited)
   * @param {string} [options.continueFrom=''] - Continuation token for pagination
   * @returns {Promise<Object>} Response with objects, commonPrefixes, nextToken, and truncated flag
   */
  async listWithOptions(options = {}) {
    return new Promise((resolve, reject) => {
      // Validate options
      if (options.prefix !== undefined && typeof options.prefix !== 'string') {
        reject(new Error('prefix must be a string'));
        return;
      }
      if (options.delimiter !== undefined && typeof options.delimiter !== 'string') {
        reject(new Error('delimiter must be a string'));
        return;
      }
      if (options.maxResults !== undefined && typeof options.maxResults !== 'number') {
        reject(new Error('maxResults must be a number'));
        return;
      }
      if (options.continueFrom !== undefined && typeof options.continueFrom !== 'string') {
        reject(new Error('continueFrom must be a string'));
        return;
      }

      // Ensure client is initialized
      this._ensureClient();

      const request = {
        prefix: options.prefix || '',
        delimiter: options.delimiter || '',
        max_results: options.maxResults || 0,
        continue_from: options.continueFrom || '',
      };

      this.client.List(request, (error, response) => {
        if (error) {
          reject(this._convertError(error));
          return;
        }

        resolve({
          objects: response.objects || [],
          commonPrefixes: response.common_prefixes || [],
          nextToken: response.next_token || '',
          truncated: response.truncated || false,
        });
      });
    });
  }

  /**
   * Check if an object exists
   * @param {string} key - Storage key for the object
   * @returns {Promise<Object>} Response with exists status
   */
  async exists(key) {
    return new Promise((resolve, reject) => {
      if (!key) {
        reject(new Error('key is required'));
        return;
      }

      // Ensure client is initialized
      this._ensureClient();

      this.client.Exists({ key }, (error, response) => {
        if (error) {
          reject(this._convertError(error));
          return;
        }

        resolve({
          exists: response.exists,
        });
      });
    });
  }

  /**
   * Get metadata for an object
   * @param {string} key - Storage key for the object
   * @returns {Promise<Object>} Response with metadata
   */
  async getMetadata(key) {
    return new Promise((resolve, reject) => {
      if (!key) {
        reject(new Error('key is required'));
        return;
      }

      // Ensure client is initialized
      this._ensureClient();

      this.client.GetMetadata({ key }, (error, response) => {
        if (error) {
          reject(this._convertError(error));
          return;
        }

        resolve({
          success: response.success,
          metadata: this._parseMetadata(response.metadata),
          message: response.message,
        });
      });
    });
  }

  /**
   * Update metadata for an object
   * @param {string} key - Storage key for the object
   * @param {Object} metadata - New metadata
   * @returns {Promise<Object>} Response with success status
   */
  async updateMetadata(key, metadata) {
    return new Promise((resolve, reject) => {
      if (!key) {
        reject(new Error('key is required'));
        return;
      }
      if (!metadata) {
        reject(new Error('metadata is required'));
        return;
      }

      // Ensure client is initialized
      this._ensureClient();

      const request = {
        key,
        metadata: this._convertMetadata(metadata),
      };

      this.client.UpdateMetadata(request, (error, response) => {
        if (error) {
          reject(this._convertError(error));
          return;
        }

        resolve({
          success: response.success,
          message: response.message,
        });
      });
    });
  }

  /**
   * Health check
   * @param {string} [service] - Optional service name
   * @returns {Promise<Object>} Health status
   */
  async health(service = '') {
    return new Promise((resolve, reject) => {
      // Ensure client is initialized
      this._ensureClient();

      this.client.Health({ service }, (error, response) => {
        if (error) {
          reject(this._convertError(error));
          return;
        }

        resolve({
          status: this._getHealthStatus(response.status),
          message: response.message,
        });
      });
    });
  }

  /**
   * Archive an object to archival storage
   * @param {string} key - Storage key for the object
   * @param {string} destinationType - Destination backend type
   * @param {Object} [destinationSettings] - Destination backend settings
   * @returns {Promise<Object>} Response with success status
   */
  async archive(key, destinationType, destinationSettings = {}) {
    return new Promise((resolve, reject) => {
      if (!key) {
        reject(new Error('key is required'));
        return;
      }
      if (!destinationType) {
        reject(new Error('destinationType is required'));
        return;
      }

      // Ensure client is initialized
      this._ensureClient();

      const request = {
        key,
        destination_type: destinationType,
        destination_settings: destinationSettings,
      };

      this.client.Archive(request, (error, response) => {
        if (error) {
          reject(this._convertError(error));
          return;
        }

        resolve({
          success: response.success,
          message: response.message,
        });
      });
    });
  }

  /**
   * Add a lifecycle policy
   * @param {Object} policy - Lifecycle policy
   * @returns {Promise<Object>} Response with success status
   */
  async addPolicy(policy) {
    return new Promise((resolve, reject) => {
      if (!policy) {
        reject(new Error('policy is required'));
        return;
      }

      // Ensure client is initialized
      this._ensureClient();

      this.client.AddPolicy({ policy }, (error, response) => {
        if (error) {
          reject(this._convertError(error));
          return;
        }

        resolve({
          success: response.success,
          message: response.message,
        });
      });
    });
  }

  /**
   * Remove a lifecycle policy
   * @param {string} id - Policy ID
   * @returns {Promise<Object>} Response with success status
   */
  async removePolicy(id) {
    return new Promise((resolve, reject) => {
      if (!id) {
        reject(new Error('id is required'));
        return;
      }

      // Ensure client is initialized
      this._ensureClient();

      this.client.RemovePolicy({ id }, (error, response) => {
        if (error) {
          reject(this._convertError(error));
          return;
        }

        resolve({
          success: response.success,
          message: response.message,
        });
      });
    });
  }

  /**
   * Get all lifecycle policies
   * @param {string} [prefix] - Optional prefix filter
   * @returns {Promise<Object>} Response with policies list
   */
  async getPolicies(prefix = '') {
    return new Promise((resolve, reject) => {
      // Ensure client is initialized
      this._ensureClient();

      this.client.GetPolicies({ prefix }, (error, response) => {
        if (error) {
          reject(this._convertError(error));
          return;
        }

        resolve({
          success: response.success,
          policies: response.policies || [],
          message: response.message,
        });
      });
    });
  }

  /**
   * Apply all lifecycle policies
   * @returns {Promise<Object>} Response with apply results
   */
  async applyPolicies() {
    return new Promise((resolve, reject) => {
      // Ensure client is initialized
      this._ensureClient();

      this.client.ApplyPolicies({}, (error, response) => {
        if (error) {
          reject(this._convertError(error));
          return;
        }

        resolve({
          success: response.success,
          policiesCount: response.policies_count,
          objectsProcessed: response.objects_processed,
          message: response.message,
        });
      });
    });
  }

  /**
   * Add a replication policy
   * @param {Object} policy - Replication policy
   * @returns {Promise<Object>} Response with success status
   */
  async addReplicationPolicy(policy) {
    return new Promise((resolve, reject) => {
      if (!policy) {
        reject(new Error('policy is required'));
        return;
      }

      // Ensure client is initialized
      this._ensureClient();

      this.client.AddReplicationPolicy({ policy }, (error, response) => {
        if (error) {
          reject(this._convertError(error));
          return;
        }

        resolve({
          success: response.success,
          message: response.message,
        });
      });
    });
  }

  /**
   * Remove a replication policy
   * @param {string} id - Policy ID
   * @returns {Promise<Object>} Response with success status
   */
  async removeReplicationPolicy(id) {
    return new Promise((resolve, reject) => {
      if (!id) {
        reject(new Error('id is required'));
        return;
      }

      // Ensure client is initialized
      this._ensureClient();

      this.client.RemoveReplicationPolicy({ id }, (error, response) => {
        if (error) {
          reject(this._convertError(error));
          return;
        }

        resolve({
          success: response.success,
          message: response.message,
        });
      });
    });
  }

  /**
   * Get all replication policies
   * @returns {Promise<Object>} Response with policies list
   */
  async getReplicationPolicies() {
    return new Promise((resolve, reject) => {
      // Ensure client is initialized
      this._ensureClient();

      this.client.GetReplicationPolicies({}, (error, response) => {
        if (error) {
          reject(this._convertError(error));
          return;
        }

        resolve({
          policies: response.policies || [],
        });
      });
    });
  }

  /**
   * Get a specific replication policy
   * @param {string} id - Policy ID
   * @returns {Promise<Object>} Response with policy
   */
  async getReplicationPolicy(id) {
    return new Promise((resolve, reject) => {
      if (!id) {
        reject(new Error('id is required'));
        return;
      }

      // Ensure client is initialized
      this._ensureClient();

      this.client.GetReplicationPolicy({ id }, (error, response) => {
        if (error) {
          reject(this._convertError(error));
          return;
        }

        resolve({
          policy: response.policy || null,
        });
      });
    });
  }

  /**
   * Trigger replication sync
   * @param {Object} [options] - Trigger options
   * @param {string} [options.policyId] - Policy ID to sync (empty for all)
   * @param {boolean} [options.parallel] - Use parallel workers
   * @param {number} [options.workerCount] - Number of workers
   * @returns {Promise<Object>} Response with sync results
   */
  async triggerReplication(options = {}) {
    return new Promise((resolve, reject) => {
      // Ensure client is initialized
      this._ensureClient();

      const request = {
        policy_id: options.policyId || '',
        parallel: options.parallel || false,
        worker_count: options.workerCount || 4,
      };

      this.client.TriggerReplication(request, (error, response) => {
        if (error) {
          reject(this._convertError(error));
          return;
        }

        resolve({
          success: response.success,
          result: response.result || {},
          message: response.message,
        });
      });
    });
  }

  /**
   * Get replication status
   * @param {string} id - Policy ID
   * @returns {Promise<Object>} Response with replication status
   */
  async getReplicationStatus(id) {
    return new Promise((resolve, reject) => {
      if (!id) {
        reject(new Error('id is required'));
        return;
      }

      // Ensure client is initialized
      this._ensureClient();

      this.client.GetReplicationStatus({ id }, (error, response) => {
        if (error) {
          reject(this._convertError(error));
          return;
        }

        resolve({
          success: response.success,
          status: response.status || {},
          message: response.message,
        });
      });
    });
  }

  /**
   * Close the client
   */
  close() {
    if (this.client) {
      this.client.close();
    }
  }

  // Helper methods

  _convertMetadata(metadata) {
    if (!metadata) return null;

    return {
      content_type: metadata.contentType || metadata.content_type || '',
      content_encoding: metadata.contentEncoding || metadata.content_encoding || '',
      size: metadata.size || 0,
      last_modified: metadata.lastModified || metadata.last_modified || null,
      etag: metadata.etag || '',
      custom: metadata.custom || {},
    };
  }

  _parseMetadata(metadata) {
    if (!metadata) return {};

    return {
      contentType: metadata.content_type,
      contentEncoding: metadata.content_encoding,
      size: metadata.size,
      lastModified: metadata.last_modified,
      etag: metadata.etag,
      custom: metadata.custom || {},
    };
  }

  _getHealthStatus(status) {
    const statuses = {
      0: 'UNKNOWN',
      1: 'SERVING',
      2: 'NOT_SERVING',
    };
    return statuses[status] || 'UNKNOWN';
  }

  _convertError(error) {
    const err = new Error(error.details || error.message);
    err.code = error.code;
    err.metadata = error.metadata;
    return err;
  }
}
