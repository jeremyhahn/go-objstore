import { Readable } from 'stream';
import axios, { AxiosHeaders, AxiosInstance, AxiosError } from 'axios';
import {
  ClientConfig,
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
import {
  ObjectNotFoundError,
  AuthenticationError,
  AuthorizationError,
  ValidationError,
  AlreadyExistsError,
  RateLimitError,
  ServerError,
  ConnectionError,
} from '../errors';

export class RestClient implements IObjectStoreClient {
  private client: AxiosInstance;

  constructor(config: ClientConfig) {
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
      ...config.headers,
    };
    if (config.token) {
      headers['Authorization'] = `Bearer ${config.token}`;
    }
    if (config.tenantId) {
      headers['X-Tenant-ID'] = config.tenantId;
    }
    this.client = axios.create({
      baseURL: config.baseUrl,
      timeout: config.timeout || 30000,
      headers,
    });
  }

  async put(request: PutRequest): Promise<PutResponse> {
    try {
      const headers: Record<string, string> = {};

      if (request.metadata) {
        const metadata = request.metadata;

        // Content type and encoding are carried in the standard HTTP headers.
        if (metadata.contentType) {
          headers['Content-Type'] = metadata.contentType;
        }
        if (metadata.contentEncoding) {
          headers['Content-Encoding'] = metadata.contentEncoding;
        }

        // Custom metadata is carried as a JSON object (string->string map) in
        // the X-Object-Metadata header. Omit the header when there is no custom.
        if (metadata.custom && Object.keys(metadata.custom).length > 0) {
          headers['X-Object-Metadata'] = JSON.stringify(metadata.custom);
        }
      }

      const response = await this.client.put(`/objects/${encodeURIComponent(request.key)}`, request.data, {
        headers,
      });

      return {
        success: true,
        message: response.data.message,
        etag: response.headers.etag || response.headers.ETag,
      };
    } catch (error) {
      return this.handleError(error);
    }
  }

  async get(request: GetRequest): Promise<GetResponse> {
    try {
      const response = await this.client.get(`/objects/${encodeURIComponent(request.key)}`, {
        responseType: 'arraybuffer',
      });

      const headers = response.headers as AxiosHeaders;
      const metadata: Metadata = {
        contentType: this.headerString(headers.get('content-type')),
        contentEncoding: this.headerString(headers.get('content-encoding')),
        size: parseInt(this.headerString(headers.get('content-length')) ?? '0', 10),
        etag: this.headerString(headers.get('etag')),
        lastModified: headers.get('last-modified')
          ? new Date(this.headerString(headers.get('last-modified')) as string)
          : undefined,
        custom: this.parseCustomMetadataHeader(this.headerString(headers.get('x-object-metadata'))),
      };

      return {
        data: Buffer.from(response.data),
        metadata,
      };
    } catch (error) {
      throw this.handleError(error);
    }
  }

  async delete(request: DeleteRequest): Promise<DeleteResponse> {
    try {
      const response = await this.client.delete(`/objects/${encodeURIComponent(request.key)}`);
      // The server returns 204 No Content (empty body); tolerate 200 + JSON
      // from older servers.
      return {
        success: true,
        message: response.data?.message ?? 'Object deleted successfully',
      };
    } catch (error) {
      return this.handleError(error);
    }
  }

  async list(request: ListRequest = {}): Promise<ListResponse> {
    try {
      const params: Record<string, any> = {};
      if (request.prefix) params.prefix = request.prefix;
      if (request.delimiter) params.delimiter = request.delimiter;
      if (request.maxResults) params.limit = request.maxResults;
      if (request.continueFrom) params.token = request.continueFrom;

      const response = await this.client.get('/objects', { params });

      return {
        objects: response.data.objects.map((obj: any) => ({
          key: obj.key,
          metadata: this.deserializeMetadata(obj),
        })),
        commonPrefixes: response.data.common_prefixes || [],
        nextToken: response.data.next_token,
        truncated: response.data.truncated || false,
      };
    } catch (error) {
      throw this.handleError(error);
    }
  }

  async exists(request: ExistsRequest): Promise<ExistsResponse> {
    try {
      await this.client.head(`/objects/${encodeURIComponent(request.key)}`);
      return { exists: true };
    } catch (error) {
      if (axios.isAxiosError(error) && error.response?.status === 404) {
        return { exists: false };
      }
      throw this.handleError(error);
    }
  }

  async getMetadata(request: GetMetadataRequest): Promise<MetadataResponse> {
    try {
      const response = await this.client.get(
        `/metadata/${encodeURIComponent(request.key)}`
      );

      const metadata = this.deserializeMetadata(response.data);

      // Custom metadata is carried as a JSON object in the X-Object-Metadata
      // response header; prefer it over the JSON body when present.
      const metaHeaders = response.headers as AxiosHeaders;
      const headerCustom = this.parseCustomMetadataHeader(
        this.headerString(metaHeaders.get('x-object-metadata'))
      );
      if (headerCustom) {
        metadata.custom = headerCustom;
      }
      if (metaHeaders.get('content-encoding')) {
        metadata.contentEncoding = this.headerString(metaHeaders.get('content-encoding'));
      }

      return {
        metadata,
        success: true,
      };
    } catch (error) {
      return this.handleError(error);
    }
  }

  async updateMetadata(request: UpdateMetadataRequest): Promise<UpdateMetadataResponse> {
    try {
      const response = await this.client.put(
        `/metadata/${encodeURIComponent(request.key)}`,
        this.serializeMetadata(request.metadata)
      );

      return {
        success: true,
        message: response.data.message,
      };
    } catch (error) {
      return this.handleError(error);
    }
  }

  async health(request: HealthRequest = {}): Promise<HealthResponse> {
    try {
      const response = await this.client.get('/health', {
        params: request.service ? { service: request.service } : {},
      });

      const status =
        response.data.status === 'healthy'
          ? HealthStatus.SERVING
          : response.data.status === 'unhealthy'
          ? HealthStatus.NOT_SERVING
          : HealthStatus.UNKNOWN;

      return {
        status,
        message: response.data.message,
      };
    } catch (error) {
      throw this.handleError(error);
    }
  }

  async archive(request: ArchiveRequest): Promise<ArchiveResponse> {
    try {
      const response = await this.client.post('/archive', {
        key: request.key,
        destination_type: request.destinationType,
        destination_settings: request.destinationSettings,
      });

      return {
        success: response.data.success || true,
        message: response.data.message,
      };
    } catch (error) {
      return this.handleError(error);
    }
  }

  async addPolicy(request: AddPolicyRequest): Promise<AddPolicyResponse> {
    try {
      const response = await this.client.post('/policies', {
        id: request.policy.id,
        prefix: request.policy.prefix,
        retention_seconds: request.policy.retentionSeconds,
        action: request.policy.action,
        destination_type: request.policy.destinationType,
        destination_settings: request.policy.destinationSettings,
      });

      return {
        success: response.data.success || true,
        message: response.data.message,
      };
    } catch (error) {
      return this.handleError(error);
    }
  }

  async removePolicy(request: RemovePolicyRequest): Promise<RemovePolicyResponse> {
    try {
      const response = await this.client.delete(`/policies/${encodeURIComponent(request.id)}`);

      return {
        success: response.data.success || true,
        message: response.data.message,
      };
    } catch (error) {
      return this.handleError(error);
    }
  }

  async getPolicies(request: GetPoliciesRequest = {}): Promise<GetPoliciesResponse> {
    try {
      const params = request.prefix ? { prefix: request.prefix } : {};
      const response = await this.client.get('/policies', { params });

      return {
        policies: response.data.policies.map((p: any) => ({
          id: p.id,
          prefix: p.prefix,
          retentionSeconds: p.retention_seconds,
          action: p.action,
          destinationType: p.destination_type,
          destinationSettings: p.destination_settings,
        })),
        success: response.data.success || true,
        message: response.data.message,
      };
    } catch (error) {
      return this.handleError(error);
    }
  }

  async applyPolicies(_request: ApplyPoliciesRequest = {}): Promise<ApplyPoliciesResponse> {
    try {
      const response = await this.client.post('/policies/apply', {});

      return {
        success: response.data.success || true,
        policiesCount: response.data.policies_count || 0,
        objectsProcessed: response.data.objects_processed || 0,
        message: response.data.message,
      };
    } catch (error) {
      return this.handleError(error);
    }
  }

  async addReplicationPolicy(
    request: AddReplicationPolicyRequest
  ): Promise<AddReplicationPolicyResponse> {
    try {
      const response = await this.client.post('/replication/policies', {
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
      });

      return {
        success: response.data.success || true,
        message: response.data.message,
      };
    } catch (error) {
      return this.handleError(error);
    }
  }

  async removeReplicationPolicy(
    request: RemoveReplicationPolicyRequest
  ): Promise<RemoveReplicationPolicyResponse> {
    try {
      const response = await this.client.delete(
        `/replication/policies/${encodeURIComponent(request.id)}`
      );

      return {
        success: response.data.success || true,
        message: response.data.message,
      };
    } catch (error) {
      return this.handleError(error);
    }
  }

  async getReplicationPolicies(
    _request: GetReplicationPoliciesRequest = {}
  ): Promise<GetReplicationPoliciesResponse> {
    try {
      const response = await this.client.get('/replication/policies');

      return {
        policies: response.data.policies.map((p: any) => ({
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
    } catch (error) {
      throw this.handleError(error);
    }
  }

  async getReplicationPolicy(
    request: GetReplicationPolicyRequest
  ): Promise<GetReplicationPolicyResponse> {
    try {
      const response = await this.client.get(
        `/replication/policies/${encodeURIComponent(request.id)}`
      );

      return {
        policy: {
          id: response.data.id,
          sourceBackend: response.data.source_backend,
          sourceSettings: response.data.source_settings,
          sourcePrefix: response.data.source_prefix,
          destinationBackend: response.data.destination_backend,
          destinationSettings: response.data.destination_settings,
          checkIntervalSeconds: response.data.check_interval_seconds,
          lastSyncTime: response.data.last_sync_time
            ? new Date(response.data.last_sync_time)
            : undefined,
          enabled: response.data.enabled,
          encryption: response.data.encryption,
          replicationMode: stringToReplicationMode(response.data.replication_mode),
        },
      };
    } catch (error) {
      throw this.handleError(error);
    }
  }

  async triggerReplication(
    request: TriggerReplicationRequest
  ): Promise<TriggerReplicationResponse> {
    try {
      const response = await this.client.post('/replication/trigger', {
        policy_id: request.policyId,
        parallel: request.parallel,
        worker_count: request.workerCount,
      });

      return {
        success: response.data.success || true,
        result: response.data.result
          ? {
              policyId: response.data.result.policy_id,
              synced: response.data.result.synced,
              deleted: response.data.result.deleted,
              failed: response.data.result.failed,
              bytesTotal: response.data.result.bytes_total,
              durationMs: response.data.result.duration_ms,
              errors: response.data.result.errors || [],
            }
          : undefined,
        message: response.data.message,
      };
    } catch (error) {
      return this.handleError(error);
    }
  }

  async getReplicationStatus(
    request: GetReplicationStatusRequest
  ): Promise<GetReplicationStatusResponse> {
    try {
      const response = await this.client.get(
        `/replication/status/${encodeURIComponent(request.id)}`
      );

      // The server returns ReplicationStatusResponse fields at the top level of
      // the response body (not wrapped in a "status" key).
      const d = response.data;
      return {
        success: d.success !== undefined ? d.success : true,
        status: d.policy_id !== undefined
          ? {
              policyId: d.policy_id,
              sourceBackend: d.source_backend,
              destinationBackend: d.destination_backend,
              enabled: d.enabled,
              totalObjectsSynced: d.total_objects_synced,
              totalObjectsDeleted: d.total_objects_deleted,
              totalBytesSynced: d.total_bytes_synced,
              totalErrors: d.total_errors,
              lastSyncTime: d.last_sync_time ? new Date(d.last_sync_time) : undefined,
              averageSyncDurationMs: this.parseDurationToMs(d.average_sync_duration),
              syncCount: d.sync_count,
            }
          : undefined,
        message: d.message,
      };
    } catch (error) {
      return this.handleError(error);
    }
  }

  /**
   * Stream an object from the backend. Returns a Node.js Readable so large
   * objects need not be fully buffered in memory.
   */
  async getStream(key: string): Promise<Readable> {
    try {
      const response = await this.client.get(`/objects/${encodeURIComponent(key)}`, {
        responseType: 'stream',
      });
      return response.data as Readable;
    } catch (error) {
      throw this.handleError(error);
    }
  }

  /**
   * Upload a Readable stream or AsyncIterable as an object. The caller is
   * responsible for setting metadata via a preceding updateMetadata call when
   * needed, since headers cannot be derived from the stream itself.
   */
  async putStream(key: string, stream: Readable | AsyncIterable<Buffer>): Promise<PutResponse> {
    try {
      const readable = stream instanceof Readable ? stream : Readable.from(stream);
      const response = await this.client.put(
        `/objects/${encodeURIComponent(key)}`,
        readable,
        {
          headers: { 'Content-Type': 'application/octet-stream' },
          maxBodyLength: Infinity,
          maxContentLength: Infinity,
        }
      );
      return {
        success: true,
        message: response.data.message,
        etag: response.headers.etag || response.headers.ETag,
      };
    } catch (error) {
      return this.handleError(error);
    }
  }

  async close(): Promise<void> {
    // Nothing to close for REST client
    return Promise.resolve();
  }

  private serializeMetadata(metadata: Metadata): any {
    return {
      content_type: metadata.contentType,
      content_encoding: metadata.contentEncoding,
      size: metadata.size,
      last_modified: metadata.lastModified?.toISOString(),
      etag: metadata.etag,
      custom: metadata.custom,
    };
  }

  // headerString extracts a plain string from an AxiosHeaderValue, which may be
  // a string, number, boolean, string[], AxiosHeaders, or null. Returns undefined
  // for any non-string-representable or absent value.
  private headerString(value: import('axios').AxiosHeaderValue): string | undefined {
    if (value === null || value === undefined) return undefined;
    if (typeof value === 'string') return value;
    return undefined;
  }

  // parseCustomMetadataHeader parses the X-Object-Metadata response header,
  // which carries the custom string->string map as a JSON object. Returns
  // undefined when the header is absent or cannot be parsed.
  private parseCustomMetadataHeader(
    header: string | undefined
  ): Record<string, string> | undefined {
    if (!header) {
      return undefined;
    }
    try {
      const parsed = JSON.parse(header);
      if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
        return parsed as Record<string, string>;
      }
    } catch {
      // Ignore malformed header and fall back to undefined.
    }
    return undefined;
  }

  private deserializeMetadata(obj: any): Metadata {
    return {
      contentType: obj.content_type || obj.metadata?.content_type,
      contentEncoding: obj.content_encoding || obj.metadata?.content_encoding,
      size: obj.size || obj.metadata?.size,
      lastModified:
        obj.last_modified || obj.modified
          ? new Date(obj.last_modified || obj.modified)
          : undefined,
      etag: obj.etag || obj.metadata?.etag,
      custom: obj.custom || obj.metadata?.custom || obj.metadata,
    };
  }

  // parseDurationToMs converts a Go time.Duration string (e.g. "1.5ms", "2s",
  // "500µs", "300ns") to a millisecond number. Returns 0 for absent/unparseable
  // values so callers always receive a valid number.
  private parseDurationToMs(duration: string | undefined): number {
    if (!duration) return 0;
    // Go duration format: a decimal number with unit suffix.
    // Units: ns, µs (or us), ms, s, m, h
    const match = duration.match(/^(-?[0-9]*\.?[0-9]+)(ns|µs|us|ms|s|m|h)$/);
    if (!match) return 0;
    const value = parseFloat(match[1]);
    switch (match[2]) {
      case 'ns': return value / 1e6;
      case 'µs':
      case 'us': return value / 1e3;
      case 'ms': return value;
      case 's':  return value * 1e3;
      case 'm':  return value * 60e3;
      case 'h':  return value * 3600e3;
      default:   return 0;
    }
  }

  // handleError converts an HTTP error response into the matching typed SDK
  // error, preserving the status code and server-provided message. Failures
  // without an HTTP response (DNS, refused connection, timeout, ...) are
  // surfaced as ConnectionError.
  private handleError(error: unknown): never {
    if (axios.isAxiosError(error)) {
      const axiosError = error as AxiosError;
      if (!axiosError.response) {
        throw new ConnectionError(`REST connection error: ${axiosError.message}`);
      }
      const message =
        (axiosError.response.data as any)?.message ||
        (axiosError.response.data as any)?.error ||
        axiosError.message;
      const status = axiosError.response.status;
      const detail = `REST API error (${status}): ${message}`;
      switch (status) {
        case 400:
          throw new ValidationError(detail);
        case 401:
          throw new AuthenticationError(detail);
        case 403:
          throw new AuthorizationError(detail);
        case 404:
          throw new ObjectNotFoundError(detail);
        case 409:
          throw new AlreadyExistsError(detail);
        case 429:
          throw new RateLimitError(detail);
        default:
          throw new ServerError(detail, status);
      }
    }
    throw error;
  }
}
