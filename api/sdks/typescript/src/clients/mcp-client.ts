import axios, { AxiosInstance, AxiosError } from 'axios';
import {
  McpClientConfig,
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
  replicationModeToString,
  stringToReplicationMode,
} from '../types';
import {
  validatePutRequest,
  validateGetRequest,
  validateDeleteRequest,
  validateListRequest,
  validateExistsRequest,
  validateGetMetadataRequest,
  validateUpdateMetadataRequest,
  validateArchiveRequest,
  validateAddPolicyRequest,
  validateRemovePolicyRequest,
  validateAddReplicationPolicyRequest,
  validateRemoveReplicationPolicyRequest,
  validateGetReplicationPolicyRequest,
  validateGetReplicationStatusRequest,
} from '../validation';
import { errorFromJsonRpc } from './jsonrpc';
import type { JsonRpcRequest, JsonRpcResponse } from './jsonrpc';

// JSON-RPC 2.0 envelope types shared with the Unix client (see ./jsonrpc),
// specialized for the MCP tools/call shape.
type McpJsonRpcRequest = JsonRpcRequest<{
  name: string;
  arguments: Record<string, unknown>;
}>;

type McpJsonRpcResponse = JsonRpcResponse<{
  content?: Array<{ type: string; text: string }>;
}>;

/**
 * McpClient communicates with go-objstore via the MCP HTTP transport
 * (pkg/server/mcp). Each operation calls HTTP POST to the base URL path "/"
 * with a JSON-RPC 2.0 envelope: method "tools/call", params { name, arguments }.
 * The tool result is a JSON string at result.content[0].text.
 */
export class McpClient implements IObjectStoreClient {
  private http: AxiosInstance;
  private nextId: number;

  constructor(config: McpClientConfig) {
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
    this.http = axios.create({
      baseURL: config.baseUrl,
      timeout: config.timeout ?? 30000,
      headers,
    });
    this.nextId = 1;
  }

  // callTool sends a tools/call JSON-RPC 2.0 request and returns the parsed
  // result object extracted from result.content[0].text.
  private async callTool(name: string, args: Record<string, unknown>): Promise<unknown> {
    const id = this.nextId++;
    const req: McpJsonRpcRequest = {
      jsonrpc: '2.0',
      method: 'tools/call',
      params: { name, arguments: args },
      id,
    };

    let resp: McpJsonRpcResponse;
    try {
      const axiosResp = await this.http.post<McpJsonRpcResponse>('/', req);
      resp = axiosResp.data;
    } catch (error) {
      throw this.handleAxiosError(error);
    }

    if (resp.error) {
      throw errorFromJsonRpc(resp.error, 'MCP');
    }

    const textContent = resp.result?.content?.[0]?.text;
    if (textContent === undefined) {
      return {};
    }

    try {
      return JSON.parse(textContent) as unknown;
    } catch {
      // Some tools return plain text; wrap it.
      return { text: textContent };
    }
  }

  async put(request: PutRequest): Promise<PutResponse> {
    validatePutRequest(request);
    const args: Record<string, unknown> = {
      key: request.key,
      data: request.data.toString('base64'),
    };
    if (request.metadata) {
      args.metadata = {
        content_type: request.metadata.contentType,
        content_encoding: request.metadata.contentEncoding,
        custom: request.metadata.custom,
      };
    }
    await this.callTool('objstore_put', args);
    return { success: true };
  }

  async get(request: GetRequest): Promise<GetResponse> {
    validateGetRequest(request);
    const result = (await this.callTool('objstore_get', { key: request.key })) as {
      data?: string;
      size?: number;
    };
    const raw = result.data ?? '';
    // The MCP server returns the object data base64-encoded in the tool result
    // JSON (matching how put encodes it); decode it back to the raw bytes.
    const data = Buffer.from(raw, 'base64');
    return { data, metadata: { size: result.size ?? data.length } };
  }

  async delete(request: DeleteRequest): Promise<DeleteResponse> {
    validateDeleteRequest(request);
    await this.callTool('objstore_delete', { key: request.key });
    return { success: true };
  }

  async list(request: ListRequest = {}): Promise<ListResponse> {
    validateListRequest(request);
    const args: Record<string, unknown> = {};
    if (request.prefix !== undefined) args.prefix = request.prefix;
    if (request.maxResults !== undefined) args.max_results = request.maxResults;
    if (request.continueFrom !== undefined) args.continue_from = request.continueFrom;

    const result = (await this.callTool('objstore_list', args)) as {
      keys?: string[];
      truncated?: boolean;
      next_token?: string;
    };
    return {
      objects: (result.keys ?? []).map((k) => ({ key: k })),
      commonPrefixes: [],
      nextToken: result.next_token,
      truncated: result.truncated ?? false,
    };
  }

  async exists(request: ExistsRequest): Promise<ExistsResponse> {
    validateExistsRequest(request);
    const result = (await this.callTool('objstore_exists', { key: request.key })) as {
      exists?: boolean;
    };
    return { exists: result.exists ?? false };
  }

  async getMetadata(request: GetMetadataRequest): Promise<MetadataResponse> {
    validateGetMetadataRequest(request);
    const result = (await this.callTool('objstore_get_metadata', { key: request.key })) as {
      size?: number;
      content_type?: string;
      content_encoding?: string;
      last_modified?: string;
      etag?: string;
      custom?: Record<string, string>;
    };
    return {
      success: true,
      metadata: {
        size: result.size,
        contentType: result.content_type,
        contentEncoding: result.content_encoding,
        lastModified: result.last_modified ? new Date(result.last_modified) : undefined,
        etag: result.etag,
        custom: result.custom,
      },
    };
  }

  async updateMetadata(request: UpdateMetadataRequest): Promise<UpdateMetadataResponse> {
    validateUpdateMetadataRequest(request);
    await this.callTool('objstore_update_metadata', {
      key: request.key,
      metadata: {
        content_type: request.metadata.contentType,
        content_encoding: request.metadata.contentEncoding,
        custom: request.metadata.custom,
      },
    });
    return { success: true };
  }

  async health(_request: HealthRequest = {}): Promise<HealthResponse> {
    const result = (await this.callTool('objstore_health', {})) as {
      status?: string;
    };
    const status =
      result.status === 'healthy'
        ? HealthStatus.SERVING
        : result.status === 'unhealthy'
        ? HealthStatus.NOT_SERVING
        : HealthStatus.UNKNOWN;
    return { status };
  }

  async archive(request: ArchiveRequest): Promise<ArchiveResponse> {
    validateArchiveRequest(request);
    await this.callTool('objstore_archive', {
      key: request.key,
      destination_type: request.destinationType,
      destination_settings: request.destinationSettings ?? {},
    });
    return { success: true };
  }

  async addPolicy(request: AddPolicyRequest): Promise<AddPolicyResponse> {
    validateAddPolicyRequest(request);
    await this.callTool('objstore_add_policy', {
      id: request.policy.id,
      prefix: request.policy.prefix,
      retention_seconds: request.policy.retentionSeconds,
      action: request.policy.action,
      destination_type: request.policy.destinationType,
      destination_settings: request.policy.destinationSettings,
    });
    return { success: true };
  }

  async removePolicy(request: RemovePolicyRequest): Promise<RemovePolicyResponse> {
    validateRemovePolicyRequest(request);
    await this.callTool('objstore_remove_policy', { id: request.id });
    return { success: true };
  }

  async getPolicies(request: GetPoliciesRequest = {}): Promise<GetPoliciesResponse> {
    const args: Record<string, unknown> = {};
    if (request.prefix !== undefined) args.prefix = request.prefix;
    const result = (await this.callTool('objstore_get_policies', args)) as {
      policies?: Array<{
        id: string;
        prefix?: string;
        action?: string;
        retention_seconds?: number;
        destination_type?: string;
        destination_settings?: Record<string, string>;
      }>;
    };
    return {
      success: true,
      policies: (result.policies ?? []).map((p) => ({
        id: p.id,
        prefix: p.prefix ?? '',
        action: p.action ?? '',
        retentionSeconds: p.retention_seconds ?? 0,
        destinationType: p.destination_type,
        destinationSettings: p.destination_settings,
      })),
    };
  }

  async applyPolicies(_request: ApplyPoliciesRequest = {}): Promise<ApplyPoliciesResponse> {
    const result = (await this.callTool('objstore_apply_policies', {})) as {
      policies_count?: number;
      objects_processed?: number;
    };
    return {
      success: true,
      policiesCount: result.policies_count ?? 0,
      objectsProcessed: result.objects_processed ?? 0,
    };
  }

  async addReplicationPolicy(
    request: AddReplicationPolicyRequest
  ): Promise<AddReplicationPolicyResponse> {
    validateAddReplicationPolicyRequest(request);
    await this.callTool('objstore_add_replication_policy', {
      id: request.policy.id,
      source_backend: request.policy.sourceBackend,
      source_settings: request.policy.sourceSettings,
      source_prefix: request.policy.sourcePrefix,
      destination_backend: request.policy.destinationBackend,
      destination_settings: request.policy.destinationSettings,
      check_interval: request.policy.checkIntervalSeconds,
      enabled: request.policy.enabled,
      replication_mode: replicationModeToString(request.policy.replicationMode),
      encryption: request.policy.encryption,
    });
    return { success: true };
  }

  async removeReplicationPolicy(
    request: RemoveReplicationPolicyRequest
  ): Promise<RemoveReplicationPolicyResponse> {
    validateRemoveReplicationPolicyRequest(request);
    await this.callTool('objstore_remove_replication_policy', { id: request.id });
    return { success: true };
  }

  async getReplicationPolicies(
    _request: GetReplicationPoliciesRequest = {}
  ): Promise<GetReplicationPoliciesResponse> {
    const result = (await this.callTool('objstore_list_replication_policies', {})) as {
      policies?: Array<{
        id: string;
        source_backend?: string;
        source_settings?: Record<string, string>;
        source_prefix?: string;
        destination_backend?: string;
        destination_settings?: Record<string, string>;
        check_interval?: number;
        last_sync_time?: string;
        enabled?: boolean;
        replication_mode?: string;
      }>;
    };
    return {
      policies: (result.policies ?? []).map((p) => ({
        id: p.id,
        sourceBackend: p.source_backend ?? '',
        sourceSettings: p.source_settings ?? {},
        sourcePrefix: p.source_prefix ?? '',
        destinationBackend: p.destination_backend ?? '',
        destinationSettings: p.destination_settings ?? {},
        checkIntervalSeconds: p.check_interval ?? 0,
        lastSyncTime: p.last_sync_time ? new Date(p.last_sync_time) : undefined,
        enabled: p.enabled ?? false,
        replicationMode: stringToReplicationMode(p.replication_mode ?? 'transparent'),
      })),
    };
  }

  async getReplicationPolicy(
    request: GetReplicationPolicyRequest
  ): Promise<GetReplicationPolicyResponse> {
    validateGetReplicationPolicyRequest(request);
    const result = (await this.callTool('objstore_get_replication_policy', {
      id: request.id,
    })) as {
      id?: string;
      source_backend?: string;
      source_settings?: Record<string, string>;
      source_prefix?: string;
      destination_backend?: string;
      destination_settings?: Record<string, string>;
      check_interval?: number;
      last_sync_time?: string;
      enabled?: boolean;
      replication_mode?: string;
    };
    return {
      policy: {
        id: result.id ?? request.id,
        sourceBackend: result.source_backend ?? '',
        sourceSettings: result.source_settings ?? {},
        sourcePrefix: result.source_prefix ?? '',
        destinationBackend: result.destination_backend ?? '',
        destinationSettings: result.destination_settings ?? {},
        checkIntervalSeconds: result.check_interval ?? 0,
        lastSyncTime: result.last_sync_time ? new Date(result.last_sync_time) : undefined,
        enabled: result.enabled ?? false,
        replicationMode: stringToReplicationMode(result.replication_mode ?? 'transparent'),
      },
    };
  }

  async triggerReplication(
    request: TriggerReplicationRequest
  ): Promise<TriggerReplicationResponse> {
    const args: Record<string, unknown> = {};
    if (request.policyId) args.policy_id = request.policyId;
    const result = (await this.callTool('objstore_trigger_replication', args)) as {
      result?: {
        policy_id?: string;
        synced?: number;
        deleted?: number;
        failed?: number;
        bytes_total?: number;
        duration?: string;
        errors?: string[];
      };
    };
    const r = result.result;
    return {
      success: true,
      result: r
        ? {
            policyId: r.policy_id ?? request.policyId ?? '',
            synced: r.synced ?? 0,
            deleted: r.deleted ?? 0,
            failed: r.failed ?? 0,
            bytesTotal: r.bytes_total ?? 0,
            durationMs: 0,
            errors: r.errors ?? [],
          }
        : undefined,
    };
  }

  async getReplicationStatus(
    request: GetReplicationStatusRequest
  ): Promise<GetReplicationStatusResponse> {
    validateGetReplicationStatusRequest(request);
    const result = (await this.callTool('objstore_get_replication_status', {
      policy_id: request.id,
    })) as {
      policy_id?: string;
      source_backend?: string;
      destination_backend?: string;
      enabled?: boolean;
      total_objects_synced?: number;
      total_objects_deleted?: number;
      total_bytes_synced?: number;
      total_errors?: number;
      last_sync_time?: string;
      average_sync_duration?: string;
      sync_count?: number;
    };
    return {
      success: true,
      status: {
        policyId: result.policy_id ?? request.id,
        sourceBackend: result.source_backend ?? '',
        destinationBackend: result.destination_backend ?? '',
        enabled: result.enabled ?? true,
        totalObjectsSynced: result.total_objects_synced ?? 0,
        totalObjectsDeleted: result.total_objects_deleted ?? 0,
        totalBytesSynced: result.total_bytes_synced ?? 0,
        totalErrors: result.total_errors ?? 0,
        lastSyncTime: result.last_sync_time ? new Date(result.last_sync_time) : undefined,
        averageSyncDurationMs: 0,
        syncCount: result.sync_count ?? 0,
      },
    };
  }

  async close(): Promise<void> {
    // Nothing to close for the MCP HTTP client.
    return Promise.resolve();
  }

  private handleAxiosError(error: unknown): Error {
    if (axios.isAxiosError(error)) {
      const axiosError = error as AxiosError;
      const message =
        (axiosError.response?.data as Record<string, unknown>)?.message ||
        (axiosError.response?.data as Record<string, unknown>)?.error ||
        axiosError.message;
      const status = axiosError.response?.status ?? 500;
      return new Error(`MCP HTTP error (${status}): ${message}`);
    }
    return error instanceof Error ? error : new Error(String(error));
  }
}
