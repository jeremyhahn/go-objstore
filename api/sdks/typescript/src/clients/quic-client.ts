import {
  QuicClientConfig,
  IObjectStoreClient,
  PutRequest,
  PutResponse,
  GetRequest,
  GetResponse,
  DeleteRequest,
  DeleteResponse,
  ListRequest,
  ListResponse,
  ExistsRequest,
  ExistsResponse,
  GetMetadataRequest,
  MetadataResponse,
  UpdateMetadataRequest,
  UpdateMetadataResponse,
  HealthRequest,
  HealthResponse,
  HealthStatus,
  ArchiveRequest,
  ArchiveResponse,
  AddPolicyRequest,
  AddPolicyResponse,
  RemovePolicyRequest,
  RemovePolicyResponse,
  GetPoliciesRequest,
  GetPoliciesResponse,
  ApplyPoliciesRequest,
  ApplyPoliciesResponse,
  AddReplicationPolicyRequest,
  AddReplicationPolicyResponse,
  RemoveReplicationPolicyRequest,
  RemoveReplicationPolicyResponse,
  GetReplicationPoliciesRequest,
  GetReplicationPoliciesResponse,
  GetReplicationPolicyRequest,
  GetReplicationPolicyResponse,
  TriggerReplicationRequest,
  TriggerReplicationResponse,
  GetReplicationStatusRequest,
  GetReplicationStatusResponse,
  Metadata,
  replicationModeToString,
  stringToReplicationMode,
} from '../types';

/**
 * QUIC/HTTP3 client for go-objstore
 *
 * Note: This is a simplified implementation. Full HTTP/3 support requires
 * a native HTTP/3 library or using fetch API with HTTP/3 support in Node.js.
 * For production use, consider using a dedicated HTTP/3 library or the
 * native fetch API when HTTP/3 support is stable.
 */
export class QuicClient implements IObjectStoreClient {
  private baseUrl: string;

  constructor(config: QuicClientConfig) {
    this.baseUrl = `${config.secure ? 'https' : 'http'}://${config.address}`;
  }

  async put(request: PutRequest): Promise<PutResponse> {
    const response = await this.makeRequest('PUT', `/objects/${encodeURIComponent(request.key)}`, {
      body: request.data,
      metadata: request.metadata,
    });

    return {
      success: true,
      message: response.message,
      etag: response.etag,
    };
  }

  async get(request: GetRequest): Promise<GetResponse> {
    // GET object returns raw binary data with metadata in headers
    const url = `${this.baseUrl}/objects/${encodeURIComponent(request.key)}`;
    const response = await fetch(url, { method: 'GET' });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`QUIC/HTTP3 error (${response.status}): ${errorText}`);
    }

    const data = Buffer.from(await response.arrayBuffer());
    const metadata: Metadata = {
      contentType: response.headers.get('content-type') || undefined,
      contentEncoding: response.headers.get('content-encoding') || undefined,
      etag: response.headers.get('etag') || undefined,
      size: parseInt(response.headers.get('content-length') || '0', 10) || data.length,
      lastModified: response.headers.get('last-modified')
        ? new Date(response.headers.get('last-modified')!)
        : undefined,
    };

    return { data, metadata };
  }

  async delete(request: DeleteRequest): Promise<DeleteResponse> {
    const response = await this.makeRequest('DELETE', `/objects/${encodeURIComponent(request.key)}`);

    return {
      success: true,
      message: response.message,
    };
  }

  async list(request: ListRequest = {}): Promise<ListResponse> {
    const params = new URLSearchParams();
    if (request.prefix) params.append('prefix', request.prefix);
    if (request.delimiter) params.append('delimiter', request.delimiter);
    if (request.maxResults) params.append('limit', request.maxResults.toString());
    if (request.continueFrom) params.append('token', request.continueFrom);

    const response = await this.makeRequest('GET', `/objects?${params.toString()}`);

    return {
      objects: response.objects.map((obj: any) => ({
        key: obj.key,
        metadata: this.deserializeMetadata(obj.metadata || obj),
      })),
      commonPrefixes: response.common_prefixes || [],
      nextToken: response.next_token,
      truncated: response.truncated || false,
    };
  }

  async exists(request: ExistsRequest): Promise<ExistsResponse> {
    try {
      const url = `${this.baseUrl}/objects/${encodeURIComponent(request.key)}`;
      const response = await fetch(url, { method: 'HEAD' });
      return { exists: response.ok };
    } catch (error) {
      return { exists: false };
    }
  }

  async getMetadata(request: GetMetadataRequest): Promise<MetadataResponse> {
    // REST API uses /metadata/:key endpoint
    const response = await this.makeRequest('GET', `/metadata/${encodeURIComponent(request.key)}`);

    return {
      metadata: this.deserializeMetadata(response),
      success: true,
    };
  }

  async updateMetadata(request: UpdateMetadataRequest): Promise<UpdateMetadataResponse> {
    // REST API uses /metadata/:key endpoint
    const response = await this.makeRequest(
      'PUT',
      `/metadata/${encodeURIComponent(request.key)}`,
      { body: this.serializeMetadata(request.metadata) }
    );

    return {
      success: true,
      message: response.message,
    };
  }

  async health(request: HealthRequest = {}): Promise<HealthResponse> {
    const params = request.service ? `?service=${request.service}` : '';
    const response = await this.makeRequest('GET', `/health${params}`);

    const status =
      response.status === 'healthy'
        ? HealthStatus.SERVING
        : response.status === 'unhealthy'
        ? HealthStatus.NOT_SERVING
        : HealthStatus.UNKNOWN;

    return {
      status,
      message: response.message,
    };
  }

  async archive(request: ArchiveRequest): Promise<ArchiveResponse> {
    const response = await this.makeRequest('POST', '/archive', {
      body: {
        key: request.key,
        destination_type: request.destinationType,
        destination_settings: request.destinationSettings,
      },
    });

    return {
      success: response.success || true,
      message: response.message,
    };
  }

  async addPolicy(request: AddPolicyRequest): Promise<AddPolicyResponse> {
    const response = await this.makeRequest('POST', '/policies', {
      body: {
        id: request.policy.id,
        prefix: request.policy.prefix,
        retention_seconds: request.policy.retentionSeconds,
        action: request.policy.action,
        destination_type: request.policy.destinationType,
        destination_settings: request.policy.destinationSettings,
      },
    });

    return {
      success: response.success || true,
      message: response.message,
    };
  }

  async removePolicy(request: RemovePolicyRequest): Promise<RemovePolicyResponse> {
    const response = await this.makeRequest('DELETE', `/policies/${encodeURIComponent(request.id)}`);

    return {
      success: response.success || true,
      message: response.message,
    };
  }

  async getPolicies(request: GetPoliciesRequest = {}): Promise<GetPoliciesResponse> {
    const params = request.prefix ? `?prefix=${request.prefix}` : '';
    const response = await this.makeRequest('GET', `/policies${params}`);

    return {
      policies: response.policies.map((p: any) => ({
        id: p.id,
        prefix: p.prefix,
        retentionSeconds: p.retention_seconds,
        action: p.action,
        destinationType: p.destination_type,
        destinationSettings: p.destination_settings,
      })),
      success: response.success || true,
      message: response.message,
    };
  }

  async applyPolicies(_request: ApplyPoliciesRequest = {}): Promise<ApplyPoliciesResponse> {
    const response = await this.makeRequest('POST', '/policies/apply', { body: {} });

    return {
      success: response.success || true,
      policiesCount: response.policies_count || 0,
      objectsProcessed: response.objects_processed || 0,
      message: response.message,
    };
  }

  async addReplicationPolicy(
    request: AddReplicationPolicyRequest
  ): Promise<AddReplicationPolicyResponse> {
    const response = await this.makeRequest('POST', '/replication/policies', {
      body: {
        id: request.policy.id,
        source_backend: request.policy.sourceBackend,
        source_settings: request.policy.sourceSettings,
        source_prefix: request.policy.sourcePrefix,
        destination_backend: request.policy.destinationBackend,
        destination_settings: request.policy.destinationSettings,
        check_interval_seconds: request.policy.checkIntervalSeconds,
        enabled: request.policy.enabled,
        encryption: request.policy.encryption,
        replication_mode: replicationModeToString(request.policy.replicationMode),
      },
    });

    return {
      success: response.success || true,
      message: response.message,
    };
  }

  async removeReplicationPolicy(
    request: RemoveReplicationPolicyRequest
  ): Promise<RemoveReplicationPolicyResponse> {
    const response = await this.makeRequest(
      'DELETE',
      `/replication/policies/${encodeURIComponent(request.id)}`
    );

    return {
      success: response.success || true,
      message: response.message,
    };
  }

  async getReplicationPolicies(
    _request: GetReplicationPoliciesRequest = {}
  ): Promise<GetReplicationPoliciesResponse> {
    const response = await this.makeRequest('GET', '/replication/policies');

    return {
      policies: response.policies.map((p: any) => ({
        id: p.id,
        sourceBackend: p.source_backend,
        sourceSettings: p.source_settings,
        sourcePrefix: p.source_prefix,
        destinationBackend: p.destination_backend,
        destinationSettings: p.destination_settings,
        checkIntervalSeconds: p.check_interval_seconds,
        lastSyncTime: p.last_sync_time ? new Date(p.last_sync_time) : undefined,
        enabled: p.enabled,
        encryption: p.encryption,
        replicationMode: stringToReplicationMode(p.replication_mode),
      })),
    };
  }

  async getReplicationPolicy(
    request: GetReplicationPolicyRequest
  ): Promise<GetReplicationPolicyResponse> {
    const response = await this.makeRequest(
      'GET',
      `/replication/policies/${encodeURIComponent(request.id)}`
    );

    return {
      policy: {
        id: response.id,
        sourceBackend: response.source_backend,
        sourceSettings: response.source_settings,
        sourcePrefix: response.source_prefix,
        destinationBackend: response.destination_backend,
        destinationSettings: response.destination_settings,
        checkIntervalSeconds: response.check_interval_seconds,
        lastSyncTime: response.last_sync_time ? new Date(response.last_sync_time) : undefined,
        enabled: response.enabled,
        encryption: response.encryption,
        replicationMode: stringToReplicationMode(response.replication_mode),
      },
    };
  }

  async triggerReplication(
    request: TriggerReplicationRequest
  ): Promise<TriggerReplicationResponse> {
    const response = await this.makeRequest('POST', '/replication/trigger', {
      body: {
        policy_id: request.policyId,
        parallel: request.parallel,
        worker_count: request.workerCount,
      },
    });

    return {
      success: response.success || true,
      result: response.result
        ? {
            policyId: response.result.policy_id,
            synced: response.result.synced,
            deleted: response.result.deleted,
            failed: response.result.failed,
            bytesTotal: response.result.bytes_total,
            durationMs: response.result.duration_ms,
            errors: response.result.errors || [],
          }
        : undefined,
      message: response.message,
    };
  }

  async getReplicationStatus(
    request: GetReplicationStatusRequest
  ): Promise<GetReplicationStatusResponse> {
    const response = await this.makeRequest(
      'GET',
      `/replication/status/${encodeURIComponent(request.id)}`
    );

    return {
      success: response.success || true,
      status: response.status
        ? {
            policyId: response.status.policy_id,
            sourceBackend: response.status.source_backend,
            destinationBackend: response.status.destination_backend,
            enabled: response.status.enabled,
            totalObjectsSynced: response.status.total_objects_synced,
            totalObjectsDeleted: response.status.total_objects_deleted,
            totalBytesSynced: response.status.total_bytes_synced,
            totalErrors: response.status.total_errors,
            lastSyncTime: response.status.last_sync_time
              ? new Date(response.status.last_sync_time)
              : undefined,
            averageSyncDurationMs: response.status.average_sync_duration_ms,
            syncCount: response.status.sync_count,
          }
        : undefined,
      message: response.message,
    };
  }

  async close(): Promise<void> {
    // Nothing to close for QUIC client
    return Promise.resolve();
  }

  private async makeRequest(method: string, path: string, options: any = {}): Promise<any> {
    // This is a simplified implementation using standard HTTP/HTTPS
    // For true HTTP/3 support, use a dedicated library or native fetch with HTTP/3
    const url = `${this.baseUrl}${path}`;

    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
    };

    let body: any;
    if (options.body) {
      if (Buffer.isBuffer(options.body)) {
        body = options.body;
        headers['Content-Type'] = 'application/octet-stream';
      } else {
        body = JSON.stringify(options.body);
      }
    }

    // Using fetch API (available in Node.js 18+)
    const response = await fetch(url, {
      method,
      headers,
      body,
    });

    if (!response.ok && response.status !== 404) {
      const errorText = await response.text();
      throw new Error(`QUIC/HTTP3 error (${response.status}): ${errorText}`);
    }

    if (method === 'HEAD') {
      return {};
    }

    if (response.headers.get('content-type')?.includes('application/json')) {
      return await response.json();
    }

    return await response.text();
  }

  private serializeMetadata(metadata: Metadata): any {
    // REST API expects common.Metadata JSON format (snake_case)
    return {
      content_type: metadata.contentType,
      content_encoding: metadata.contentEncoding,
      size: metadata.size,
      last_modified: metadata.lastModified?.toISOString(),
      etag: metadata.etag,
      custom: metadata.custom,
    };
  }

  private deserializeMetadata(obj: any): Metadata {
    // REST API returns ObjectResponse format:
    // - content_type (snake_case)
    // - modified (not last_modified)
    // - size, etag, key, metadata (for custom)
    return {
      contentType: obj.content_type || obj.contentType,
      contentEncoding: obj.content_encoding || obj.contentEncoding,
      size: typeof obj.size === 'number' ? obj.size : parseInt(obj.size || '0', 10),
      lastModified: obj.modified || obj.last_modified || obj.lastModified
        ? new Date(obj.modified || obj.last_modified || obj.lastModified)
        : undefined,
      etag: obj.etag,
      custom: obj.metadata || obj.custom,
    };
  }
}
