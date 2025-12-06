import axios from 'axios';

/**
 * Convert replication mode to string format for REST API.
 * @param {number|string} mode - Replication mode (0, 1, 'transparent', 'opaque')
 * @returns {string} String representation ('transparent' or 'opaque')
 */
function normalizeReplicationMode(mode) {
  if (typeof mode === 'number') {
    return mode === 1 ? 'opaque' : 'transparent';
  }
  if (typeof mode === 'string') {
    return mode.toLowerCase() === 'opaque' ? 'opaque' : 'transparent';
  }
  return 'transparent';
}

/**
 * REST API client for go-objstore
 * Supports all object storage operations via HTTP/REST protocol
 */
export class RestClient {
  /**
   * Create a new REST client
   * @param {Object} config - Client configuration
   * @param {string} config.baseURL - Base URL of the object store server
   * @param {number} [config.timeout=30000] - Request timeout in milliseconds
   * @param {Object} [config.headers={}] - Additional headers for all requests
   */
  constructor(config) {
    if (!config?.baseURL) {
      throw new Error('baseURL is required');
    }

    this.baseURL = config.baseURL.replace(/\/$/, '');
    this.timeout = config.timeout || 30000;
    this.headers = config.headers || {};

    this.client = axios.create({
      baseURL: this.baseURL,
      timeout: this.timeout,
      headers: {
        'Content-Type': 'application/json',
        ...this.headers,
      },
    });
  }

  /**
   * Store an object in the backend
   * @param {string} key - Storage key for the object
   * @param {Buffer|Uint8Array|string} data - Object data
   * @param {Object} [metadata] - Optional metadata
   * @returns {Promise<Object>} Response with success status and etag
   */
  async put(key, data, metadata = null) {
    if (!key) throw new Error('key is required');
    if (!data) throw new Error('data is required');

    // Convert data to Buffer if necessary
    const bodyData = Buffer.isBuffer(data) ? data : Buffer.from(data);

    // Build headers
    const headers = {
      'Content-Type': metadata?.contentType || 'application/octet-stream',
    };

    if (metadata?.contentEncoding) {
      headers['Content-Encoding'] = metadata.contentEncoding;
    }

    // Send custom metadata as JSON header
    if (metadata?.custom || (metadata && Object.keys(metadata).length > 0)) {
      const metadataObj = {};
      if (metadata.contentType) metadataObj.content_type = metadata.contentType;
      if (metadata.contentEncoding) metadataObj.content_encoding = metadata.contentEncoding;
      if (metadata.custom) Object.assign(metadataObj, metadata.custom);
      headers['X-Object-Metadata'] = JSON.stringify(metadataObj);
    }

    const response = await this.client.put(`/objects/${encodeURIComponent(key)}`, bodyData, {
      headers,
    });

    return {
      success: true,
      message: response.data.message || 'Object uploaded successfully',
      etag: response.data.data?.etag || '',
    };
  }

  /**
   * Retrieve an object from the backend
   * @param {string} key - Storage key for the object
   * @returns {Promise<Object>} Response with data and metadata
   */
  async get(key) {
    if (!key) throw new Error('key is required');

    const response = await this.client.get(`/objects/${encodeURIComponent(key)}`, {
      responseType: 'arraybuffer',
    });

    const metadata = {
      contentType: response.headers['content-type'],
      contentLength: parseInt(response.headers['content-length'] || '0'),
      etag: response.headers['etag'],
      lastModified: response.headers['last-modified'],
    };

    return {
      data: Buffer.from(response.data),
      metadata,
    };
  }

  /**
   * Delete an object from the backend
   * @param {string} key - Storage key for the object
   * @returns {Promise<Object>} Response with success status
   */
  async delete(key) {
    if (!key) throw new Error('key is required');

    const response = await this.client.delete(`/objects/${encodeURIComponent(key)}`);

    return {
      success: true,
      message: response.data.message || 'Object deleted successfully',
    };
  }

  /**
   * List objects matching the criteria
   * @param {Object} [options] - List options
   * @param {string} [options.prefix] - Prefix to filter objects
   * @param {string} [options.delimiter] - Delimiter for hierarchical listing
   * @param {number} [options.limit] - Maximum number of results
   * @param {string} [options.token] - Pagination token
   * @returns {Promise<Object>} Response with objects list
   */
  async list(options = {}) {
    const params = {};
    if (options.prefix) params.prefix = options.prefix;
    if (options.delimiter) params.delimiter = options.delimiter;
    if (options.limit) params.limit = options.limit;
    if (options.token) params.token = options.token;

    const response = await this.client.get('/objects', { params });

    return {
      objects: response.data.objects || [],
      commonPrefixes: response.data.common_prefixes || [],
      nextToken: response.data.next_token || '',
      truncated: response.data.truncated || false,
    };
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
    const params = {};

    // Validate options
    if (options.prefix !== undefined && typeof options.prefix !== 'string') {
      throw new Error('prefix must be a string');
    }
    if (options.delimiter !== undefined && typeof options.delimiter !== 'string') {
      throw new Error('delimiter must be a string');
    }
    if (options.maxResults !== undefined && typeof options.maxResults !== 'number') {
      throw new Error('maxResults must be a number');
    }
    if (options.continueFrom !== undefined && typeof options.continueFrom !== 'string') {
      throw new Error('continueFrom must be a string');
    }

    if (options.prefix) params.prefix = options.prefix;
    if (options.delimiter) params.delimiter = options.delimiter;
    if (options.maxResults) params.limit = options.maxResults;
    if (options.continueFrom) params.token = options.continueFrom;

    const response = await this.client.get('/objects', { params });

    return {
      objects: response.data.objects || [],
      commonPrefixes: response.data.common_prefixes || [],
      nextToken: response.data.next_token || '',
      truncated: response.data.truncated || false,
    };
  }

  /**
   * Check if an object exists
   * @param {string} key - Storage key for the object
   * @returns {Promise<Object>} Response with exists status
   */
  async exists(key) {
    if (!key) throw new Error('key is required');

    try {
      await this.client.head(`/objects/${encodeURIComponent(key)}`);
      return { exists: true };
    } catch (error) {
      if (error.response?.status === 404) {
        return { exists: false };
      }
      throw error;
    }
  }

  /**
   * Get metadata for an object
   * @param {string} key - Storage key for the object
   * @returns {Promise<Object>} Response with metadata
   */
  async getMetadata(key) {
    if (!key) throw new Error('key is required');

    const response = await this.client.get(`/metadata/${encodeURIComponent(key)}`);

    return {
      success: true,
      metadata: response.data.metadata || {},
    };
  }

  /**
   * Update metadata for an object
   * @param {string} key - Storage key for the object
   * @param {Object} metadata - New metadata
   * @returns {Promise<Object>} Response with success status
   */
  async updateMetadata(key, metadata) {
    if (!key) throw new Error('key is required');
    if (!metadata) throw new Error('metadata is required');

    const response = await this.client.put(
      `/metadata/${encodeURIComponent(key)}`,
      metadata
    );

    return {
      success: true,
      message: response.data.message || 'Metadata updated successfully',
    };
  }

  /**
   * Health check
   * @returns {Promise<Object>} Health status
   */
  async health() {
    const response = await this.client.get('/health');

    return {
      status: response.data.status || 'unknown',
      message: response.data.message || '',
    };
  }

  /**
   * Archive an object to archival storage
   * @param {string} key - Storage key for the object
   * @param {string} destinationType - Destination backend type
   * @param {Object} [destinationSettings] - Destination backend settings
   * @returns {Promise<Object>} Response with success status
   */
  async archive(key, destinationType, destinationSettings = {}) {
    if (!key) throw new Error('key is required');
    if (!destinationType) throw new Error('destinationType is required');

    const response = await this.client.post('/archive', {
      key,
      destination_type: destinationType,
      destination_settings: destinationSettings,
    });

    return {
      success: true,
      message: response.data.message || 'Object archived successfully',
    };
  }

  /**
   * Add a lifecycle policy
   * @param {Object} policy - Lifecycle policy
   * @returns {Promise<Object>} Response with success status
   */
  async addPolicy(policy) {
    if (!policy) throw new Error('policy is required');

    const response = await this.client.post('/policies', policy);

    return {
      success: true,
      message: response.data.message || 'Policy added successfully',
    };
  }

  /**
   * Remove a lifecycle policy
   * @param {string} id - Policy ID
   * @returns {Promise<Object>} Response with success status
   */
  async removePolicy(id) {
    if (!id) throw new Error('id is required');

    const response = await this.client.delete(`/policies/${id}`);

    return {
      success: true,
      message: response.data.message || 'Policy removed successfully',
    };
  }

  /**
   * Get all lifecycle policies
   * @param {string} [prefix] - Optional prefix filter
   * @returns {Promise<Object>} Response with policies list
   */
  async getPolicies(prefix = '') {
    const params = prefix ? { prefix } : {};
    const response = await this.client.get('/policies', { params });

    return {
      success: true,
      policies: response.data.policies || [],
    };
  }

  /**
   * Apply all lifecycle policies
   * @returns {Promise<Object>} Response with apply results
   */
  async applyPolicies() {
    const response = await this.client.post('/policies/apply');

    return {
      success: true,
      policiesCount: response.data.policies_count || 0,
      objectsProcessed: response.data.objects_processed || 0,
      message: response.data.message || '',
    };
  }

  /**
   * Add a replication policy
   * @param {Object} policy - Replication policy
   * @returns {Promise<Object>} Response with success status
   */
  async addReplicationPolicy(policy) {
    if (!policy) throw new Error('policy is required');

    // Normalize replication_mode to string format expected by REST API
    const normalizedPolicy = {
      ...policy,
      replication_mode: normalizeReplicationMode(policy.replication_mode || policy.replicationMode),
    };

    const response = await this.client.post('/replication/policies', normalizedPolicy);

    return {
      success: true,
      message: response.data.message || 'Replication policy added successfully',
    };
  }

  /**
   * Remove a replication policy
   * @param {string} id - Policy ID
   * @returns {Promise<Object>} Response with success status
   */
  async removeReplicationPolicy(id) {
    if (!id) throw new Error('id is required');

    const response = await this.client.delete(`/replication/policies/${id}`);

    return {
      success: true,
      message: response.data.message || 'Replication policy removed successfully',
    };
  }

  /**
   * Get all replication policies
   * @returns {Promise<Object>} Response with policies list
   */
  async getReplicationPolicies() {
    const response = await this.client.get('/replication/policies');

    return {
      policies: response.data.policies || [],
    };
  }

  /**
   * Get a specific replication policy
   * @param {string} id - Policy ID
   * @returns {Promise<Object>} Response with policy
   */
  async getReplicationPolicy(id) {
    if (!id) throw new Error('id is required');

    const response = await this.client.get(`/replication/policies/${id}`);

    return {
      policy: response.data.policy || null,
    };
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
    const response = await this.client.post('/replication/trigger', {
      policy_id: options.policyId || '',
      parallel: options.parallel || false,
      worker_count: options.workerCount || 4,
    });

    return {
      success: true,
      result: response.data.result || {},
      message: response.data.message || '',
    };
  }

  /**
   * Get replication status
   * @param {string} id - Policy ID
   * @returns {Promise<Object>} Response with replication status
   */
  async getReplicationStatus(id) {
    if (!id) throw new Error('id is required');

    const response = await this.client.get(`/replication/status/${id}`);

    return {
      success: true,
      status: response.data.status || {},
      message: response.data.message || '',
    };
  }

  /**
   * Close the client
   */
  close() {
    // No cleanup needed for axios
  }
}
