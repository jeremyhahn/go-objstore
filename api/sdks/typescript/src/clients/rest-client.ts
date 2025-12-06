import axios, { AxiosInstance, AxiosError } from 'axios';
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

export class RestClient implements IObjectStoreClient {
  private client: AxiosInstance;

  constructor(config: ClientConfig) {
    this.client = axios.create({
      baseURL: config.baseUrl,
      timeout: config.timeout || 30000,
      headers: {
        'Content-Type': 'application/json',
        ...config.headers,
      },
    });
  }

  async put(request: PutRequest): Promise<PutResponse> {
    try {
      const headers: Record<string, string> = {};

      if (request.metadata) {
        headers['X-Metadata'] = JSON.stringify(this.serializeMetadata(request.metadata));
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

      const metadata: Metadata = {
        contentType: response.headers['content-type'],
        size: parseInt(response.headers['content-length'] || '0', 10),
        etag: response.headers.etag,
        lastModified: response.headers['last-modified']
          ? new Date(response.headers['last-modified'])
          : undefined,
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
      return {
        success: true,
        message: response.data.message,
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

      return {
        metadata: this.deserializeMetadata(response.data),
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

      return {
        success: response.data.success || true,
        status: response.data.status
          ? {
              policyId: response.data.status.policy_id,
              sourceBackend: response.data.status.source_backend,
              destinationBackend: response.data.status.destination_backend,
              enabled: response.data.status.enabled,
              totalObjectsSynced: response.data.status.total_objects_synced,
              totalObjectsDeleted: response.data.status.total_objects_deleted,
              totalBytesSynced: response.data.status.total_bytes_synced,
              totalErrors: response.data.status.total_errors,
              lastSyncTime: response.data.status.last_sync_time
                ? new Date(response.data.status.last_sync_time)
                : undefined,
              averageSyncDurationMs: response.data.status.average_sync_duration_ms,
              syncCount: response.data.status.sync_count,
            }
          : undefined,
        message: response.data.message,
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

  private handleError(error: unknown): never {
    if (axios.isAxiosError(error)) {
      const axiosError = error as AxiosError;
      const message =
        (axiosError.response?.data as any)?.message ||
        (axiosError.response?.data as any)?.error ||
        axiosError.message;
      const status = axiosError.response?.status || 500;
      throw new Error(`REST API error (${status}): ${message}`);
    }
    throw error;
  }
}
