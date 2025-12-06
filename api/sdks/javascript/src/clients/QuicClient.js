// Check for Node.js environment
const isNode = typeof process !== 'undefined' && process.versions && process.versions.node;

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
 * QUIC/HTTP3 client for go-objstore
 * Supports all object storage operations via QUIC/HTTP3 protocol
 *
 * Note: Full HTTP/3 support in Node.js is still experimental.
 * This implementation uses HTTP/2 over TLS as a fallback with the same API surface.
 * In browser environments, uses standard fetch API.
 */
export class QuicClient {
  /**
   * Create a new QUIC/HTTP3 client
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
    this.isNode = isNode;

    // Note: HTTPS agent is not created here for better browser compatibility
    // The fetch API will handle HTTPS connections automatically
    this.agent = null;
  }

  /**
   * Make an HTTP request using fetch API
   * @private
   */
  async _makeRequest(method, path, options = {}) {
    const url = `${this.baseURL}${path}`;

    const headers = {
      ...this.headers,
      ...options.headers,
    };

    const fetchOptions = {
      method,
      headers,
      signal: AbortSignal.timeout(this.timeout),
    };

    // Note: Agent is not used for better browser compatibility
    // Modern fetch API handles HTTPS automatically

    if (options.body !== undefined) {
      fetchOptions.body = options.body;
    }

    const response = await fetch(url, fetchOptions);

    if (!response.ok && response.status !== 404) {
      const errorText = await response.text();
      throw new Error(`QUIC/HTTP3 error (${response.status}): ${errorText}`);
    }

    return response;
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

    const headers = {
      'Content-Type': metadata?.contentType || 'application/octet-stream',
    };

    if (metadata?.contentEncoding) {
      headers['Content-Encoding'] = metadata.contentEncoding;
    }

    if (metadata?.custom) {
      headers['X-Metadata'] = JSON.stringify(metadata.custom);
    }

    const response = await this._makeRequest(
      'PUT',
      `/objects/${encodeURIComponent(key)}`,
      {
        headers,
        body: Buffer.from(data),
      }
    );

    const result = await response.json();
    return {
      success: true,
      message: result.message || 'Object uploaded successfully',
      etag: result.data?.etag || result.etag || '',
    };
  }

  /**
   * Retrieve an object from the backend
   * @param {string} key - Storage key for the object
   * @returns {Promise<Object>} Response with data and metadata
   */
  async get(key) {
    if (!key) throw new Error('key is required');

    const response = await this._makeRequest('GET', `/objects/${encodeURIComponent(key)}`);

    const metadata = {
      contentType: response.headers.get('content-type'),
      contentLength: parseInt(response.headers.get('content-length') || '0'),
      etag: response.headers.get('etag'),
      lastModified: response.headers.get('last-modified'),
    };

    const arrayBuffer = await response.arrayBuffer();
    return {
      data: Buffer.from(arrayBuffer),
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

    const response = await this._makeRequest('DELETE', `/objects/${encodeURIComponent(key)}`);
    const result = await response.json();

    return {
      success: true,
      message: result.message || 'Object deleted successfully',
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
    const params = new URLSearchParams();
    if (options.prefix) params.append('prefix', options.prefix);
    if (options.delimiter) params.append('delimiter', options.delimiter);
    if (options.limit) params.append('limit', options.limit.toString());
    if (options.token) params.append('token', options.token);

    const queryString = params.toString();
    const path = queryString ? `/objects?${queryString}` : '/objects';
    const response = await this._makeRequest('GET', path);
    const result = await response.json();

    return {
      objects: result.objects || [],
      commonPrefixes: result.common_prefixes || [],
      nextToken: result.next_token || '',
      truncated: result.truncated || false,
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

    const params = new URLSearchParams();
    if (options.prefix) params.append('prefix', options.prefix);
    if (options.delimiter) params.append('delimiter', options.delimiter);
    if (options.maxResults) params.append('limit', options.maxResults.toString());
    if (options.continueFrom) params.append('token', options.continueFrom);

    const queryString = params.toString();
    const path = queryString ? `/objects?${queryString}` : '/objects';
    const response = await this._makeRequest('GET', path);
    const result = await response.json();

    return {
      objects: result.objects || [],
      commonPrefixes: result.common_prefixes || [],
      nextToken: result.next_token || '',
      truncated: result.truncated || false,
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
      const response = await this._makeRequest('HEAD', `/objects/${encodeURIComponent(key)}`);
      return { exists: response.ok };
    } catch (error) {
      return { exists: false };
    }
  }

  /**
   * Get metadata for an object
   * @param {string} key - Storage key for the object
   * @returns {Promise<Object>} Response with metadata
   */
  async getMetadata(key) {
    if (!key) throw new Error('key is required');

    const response = await this._makeRequest('GET', `/metadata/${encodeURIComponent(key)}`);
    const result = await response.json();

    return {
      success: true,
      metadata: result.metadata || {},
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

    const response = await this._makeRequest(
      'PUT',
      `/metadata/${encodeURIComponent(key)}`,
      {
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(metadata),
      }
    );

    const result = await response.json();
    return {
      success: true,
      message: result.message || 'Metadata updated successfully',
    };
  }

  /**
   * Health check
   * @returns {Promise<Object>} Health status
   */
  async health() {
    const response = await this._makeRequest('GET', '/health');
    const result = await response.json();

    return {
      status: result.status || 'unknown',
      message: result.message || '',
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

    const response = await this._makeRequest('POST', '/archive', {
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        key,
        destination_type: destinationType,
        destination_settings: destinationSettings,
      }),
    });

    const result = await response.json();
    return {
      success: true,
      message: result.message || 'Object archived successfully',
    };
  }

  /**
   * Add a lifecycle policy
   * @param {Object} policy - Lifecycle policy
   * @returns {Promise<Object>} Response with success status
   */
  async addPolicy(policy) {
    if (!policy) throw new Error('policy is required');

    const response = await this._makeRequest('POST', '/policies', {
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(policy),
    });

    const result = await response.json();
    return {
      success: true,
      message: result.message || 'Policy added successfully',
    };
  }

  /**
   * Remove a lifecycle policy
   * @param {string} id - Policy ID
   * @returns {Promise<Object>} Response with success status
   */
  async removePolicy(id) {
    if (!id) throw new Error('id is required');

    const response = await this._makeRequest('DELETE', `/policies/${id}`);
    const result = await response.json();

    return {
      success: true,
      message: result.message || 'Policy removed successfully',
    };
  }

  /**
   * Get all lifecycle policies
   * @param {string} [prefix] - Optional prefix filter
   * @returns {Promise<Object>} Response with policies list
   */
  async getPolicies(prefix = '') {
    const path = prefix ? `/policies?prefix=${encodeURIComponent(prefix)}` : '/policies';
    const response = await this._makeRequest('GET', path);
    const result = await response.json();

    return {
      success: true,
      policies: result.policies || [],
    };
  }

  /**
   * Apply all lifecycle policies
   * @returns {Promise<Object>} Response with apply results
   */
  async applyPolicies() {
    const response = await this._makeRequest('POST', '/policies/apply', {
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    });

    const result = await response.json();
    return {
      success: true,
      policiesCount: result.policies_count || 0,
      objectsProcessed: result.objects_processed || 0,
      message: result.message || '',
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

    const response = await this._makeRequest('POST', '/replication/policies', {
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(normalizedPolicy),
    });

    const result = await response.json();
    return {
      success: true,
      message: result.message || 'Replication policy added successfully',
    };
  }

  /**
   * Remove a replication policy
   * @param {string} id - Policy ID
   * @returns {Promise<Object>} Response with success status
   */
  async removeReplicationPolicy(id) {
    if (!id) throw new Error('id is required');

    const response = await this._makeRequest('DELETE', `/replication/policies/${id}`);
    const result = await response.json();

    return {
      success: true,
      message: result.message || 'Replication policy removed successfully',
    };
  }

  /**
   * Get all replication policies
   * @returns {Promise<Object>} Response with policies list
   */
  async getReplicationPolicies() {
    const response = await this._makeRequest('GET', '/replication/policies');
    const result = await response.json();

    return {
      policies: result.policies || [],
    };
  }

  /**
   * Get a specific replication policy
   * @param {string} id - Policy ID
   * @returns {Promise<Object>} Response with policy
   */
  async getReplicationPolicy(id) {
    if (!id) throw new Error('id is required');

    const response = await this._makeRequest('GET', `/replication/policies/${id}`);
    const result = await response.json();

    return {
      policy: result.policy || null,
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
    const response = await this._makeRequest('POST', '/replication/trigger', {
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        policy_id: options.policyId || '',
        parallel: options.parallel || false,
        worker_count: options.workerCount || 4,
      }),
    });

    const result = await response.json();
    return {
      success: true,
      result: result.result || {},
      message: result.message || '',
    };
  }

  /**
   * Get replication status
   * @param {string} id - Policy ID
   * @returns {Promise<Object>} Response with replication status
   */
  async getReplicationStatus(id) {
    if (!id) throw new Error('id is required');

    const response = await this._makeRequest('GET', `/replication/status/${id}`);
    const result = await response.json();

    return {
      success: true,
      status: result.status || {},
      message: result.message || '',
    };
  }

  /**
   * Close the client
   */
  close() {
    // No cleanup needed for fetch
  }
}
