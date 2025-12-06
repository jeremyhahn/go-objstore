import { RestClient } from './clients/RestClient.js';
import { GrpcClient } from './clients/GrpcClient.js';
import { QuicClient } from './clients/QuicClient.js';

/**
 * Unified ObjectStore client supporting multiple protocols
 * Provides a consistent API across REST, gRPC, and QUIC/HTTP3 protocols
 */
export class ObjectStoreClient {
  /**
   * Create a new ObjectStore client
   * @param {Object} config - Client configuration
   * @param {string} config.protocol - Protocol to use ('rest', 'grpc', or 'quic')
   * @param {string} config.baseURL - Base URL for REST/QUIC or address for gRPC
   * @param {number} [config.timeout=30000] - Request timeout in milliseconds
   * @param {Object} [config.headers] - Additional headers (REST/QUIC only)
   * @param {boolean} [config.insecure=true] - Use insecure connection (gRPC only)
   * @param {Object} [config.credentials] - gRPC credentials (gRPC only)
   */
  constructor(config) {
    if (!config) {
      throw new Error('Configuration is required');
    }

    if (!config.protocol) {
      throw new Error('protocol is required (rest, grpc, or quic)');
    }

    this.protocol = config.protocol.toLowerCase();
    this.config = config;

    // Create the appropriate client based on protocol
    switch (this.protocol) {
      case 'rest':
        this.client = new RestClient({
          baseURL: config.baseURL,
          timeout: config.timeout,
          headers: config.headers,
        });
        break;

      case 'grpc':
        this.client = new GrpcClient({
          address: config.baseURL || config.address,
          insecure: config.insecure,
          credentials: config.credentials,
        });
        break;

      case 'quic':
      case 'http3':
        this.client = new QuicClient({
          baseURL: config.baseURL,
          timeout: config.timeout,
          headers: config.headers,
          http2: config.http2,
        });
        break;

      default:
        throw new Error(`Unsupported protocol: ${config.protocol}. Use 'rest', 'grpc', or 'quic'`);
    }
  }

  /**
   * Store an object in the backend
   * @param {string} key - Storage key for the object
   * @param {Buffer|Uint8Array|string} data - Object data
   * @param {Object} [metadata] - Optional metadata
   * @returns {Promise<Object>} Response with success status and etag
   */
  async put(key, data, metadata = null) {
    return this.client.put(key, data, metadata);
  }

  /**
   * Retrieve an object from the backend
   * @param {string} key - Storage key for the object
   * @returns {Promise<Object>} Response with data and metadata
   */
  async get(key) {
    return this.client.get(key);
  }

  /**
   * Delete an object from the backend
   * @param {string} key - Storage key for the object
   * @returns {Promise<Object>} Response with success status
   */
  async delete(key) {
    return this.client.delete(key);
  }

  /**
   * List objects matching the criteria
   * @param {Object} [options] - List options
   * @param {string} [options.prefix] - Prefix to filter objects
   * @param {string} [options.delimiter] - Delimiter for hierarchical listing
   * @param {number} [options.limit] - Maximum number of results (REST/QUIC)
   * @param {number} [options.maxResults] - Maximum number of results (gRPC)
   * @param {string} [options.token] - Pagination token (REST/QUIC)
   * @param {string} [options.continueFrom] - Pagination token (gRPC)
   * @returns {Promise<Object>} Response with objects list
   */
  async list(options = {}) {
    // Normalize options for different protocols
    if (this.protocol === 'grpc') {
      const grpcOptions = {
        prefix: options.prefix,
        delimiter: options.delimiter,
        maxResults: options.maxResults || options.limit,
        continueFrom: options.continueFrom || options.token,
      };
      return this.client.list(grpcOptions);
    }

    return this.client.list(options);
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
    return this.client.listWithOptions(options);
  }

  /**
   * Check if an object exists
   * @param {string} key - Storage key for the object
   * @returns {Promise<Object>} Response with exists status
   */
  async exists(key) {
    return this.client.exists(key);
  }

  /**
   * Get metadata for an object
   * @param {string} key - Storage key for the object
   * @returns {Promise<Object>} Response with metadata
   */
  async getMetadata(key) {
    return this.client.getMetadata(key);
  }

  /**
   * Update metadata for an object
   * @param {string} key - Storage key for the object
   * @param {Object} metadata - New metadata
   * @returns {Promise<Object>} Response with success status
   */
  async updateMetadata(key, metadata) {
    return this.client.updateMetadata(key, metadata);
  }

  /**
   * Health check
   * @param {string} [service] - Optional service name (gRPC only)
   * @returns {Promise<Object>} Health status
   */
  async health(service) {
    if (this.protocol === 'grpc') {
      return this.client.health(service);
    }
    return this.client.health();
  }

  /**
   * Archive an object to archival storage
   * @param {string} key - Storage key for the object
   * @param {string} destinationType - Destination backend type
   * @param {Object} [destinationSettings] - Destination backend settings
   * @returns {Promise<Object>} Response with success status
   */
  async archive(key, destinationType, destinationSettings = {}) {
    return this.client.archive(key, destinationType, destinationSettings);
  }

  /**
   * Add a lifecycle policy
   * @param {Object} policy - Lifecycle policy
   * @returns {Promise<Object>} Response with success status
   */
  async addPolicy(policy) {
    return this.client.addPolicy(policy);
  }

  /**
   * Remove a lifecycle policy
   * @param {string} id - Policy ID
   * @returns {Promise<Object>} Response with success status
   */
  async removePolicy(id) {
    return this.client.removePolicy(id);
  }

  /**
   * Get all lifecycle policies
   * @param {string} [prefix] - Optional prefix filter
   * @returns {Promise<Object>} Response with policies list
   */
  async getPolicies(prefix = '') {
    return this.client.getPolicies(prefix);
  }

  /**
   * Apply all lifecycle policies
   * @returns {Promise<Object>} Response with apply results
   */
  async applyPolicies() {
    return this.client.applyPolicies();
  }

  /**
   * Add a replication policy
   * @param {Object} policy - Replication policy
   * @returns {Promise<Object>} Response with success status
   */
  async addReplicationPolicy(policy) {
    return this.client.addReplicationPolicy(policy);
  }

  /**
   * Remove a replication policy
   * @param {string} id - Policy ID
   * @returns {Promise<Object>} Response with success status
   */
  async removeReplicationPolicy(id) {
    return this.client.removeReplicationPolicy(id);
  }

  /**
   * Get all replication policies
   * @returns {Promise<Object>} Response with policies list
   */
  async getReplicationPolicies() {
    return this.client.getReplicationPolicies();
  }

  /**
   * Get a specific replication policy
   * @param {string} id - Policy ID
   * @returns {Promise<Object>} Response with policy
   */
  async getReplicationPolicy(id) {
    return this.client.getReplicationPolicy(id);
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
    return this.client.triggerReplication(options);
  }

  /**
   * Get replication status
   * @param {string} id - Policy ID
   * @returns {Promise<Object>} Response with replication status
   */
  async getReplicationStatus(id) {
    return this.client.getReplicationStatus(id);
  }

  /**
   * Get the protocol being used
   * @returns {string} Protocol name
   */
  getProtocol() {
    return this.protocol;
  }

  /**
   * Get the underlying client instance
   * @returns {Object} Client instance
   */
  getClient() {
    return this.client;
  }

  /**
   * Close the client and cleanup resources
   */
  close() {
    if (this.client && typeof this.client.close === 'function') {
      this.client.close();
    }
  }
}
