import * as net from 'net';
import {
  UnixClientConfig,
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
import { ConnectionError } from '../errors';
import { errorFromJsonRpc } from './jsonrpc';
import type { JsonRpcRequest, JsonRpcResponse } from './jsonrpc';

// JSON-RPC 2.0 types matching pkg/server/unix/protocol.go
/**
 * UnixClient communicates with go-objstore over a Unix domain socket using
 * newline-delimited JSON-RPC 2.0 (pkg/server/unix). Authentication is handled
 * server-side via peercred; no credential is sent by the client.
 *
 * The server keeps connections open and serves multiple newline-delimited
 * requests per connection (with a ~30s idle read deadline). The client
 * therefore maintains ONE persistent connection, serializing request/response
 * pairs through an internal queue. When the socket errors, closes (e.g. the
 * server's idle deadline fires), or a response id mismatches, the socket is
 * destroyed and a fresh connection is dialed lazily on the next call.
 */
export class UnixClient implements IObjectStoreClient {
  private readonly socketPath: string;
  private readonly timeout: number;
  private nextId: number;
  private socket: net.Socket | null;
  private recvBuffer: string;
  // pending serializes RPCs so at most one request/response pair is in flight
  // on the shared connection at any time.
  private pending: Promise<unknown>;

  constructor(config: UnixClientConfig) {
    this.socketPath = config.socketPath;
    this.timeout = config.timeout ?? 30000;
    this.nextId = 1;
    this.socket = null;
    this.recvBuffer = '';
    this.pending = Promise.resolve();
  }

  // rpc enqueues a JSON-RPC 2.0 request behind any in-flight request and
  // returns the parsed result. Requests are strictly serialized.
  private rpc(method: string, params: unknown): Promise<unknown> {
    const run = () => this.dispatch(method, params);
    const next = this.pending.then(run, run);
    // Keep the queue alive regardless of individual call outcomes.
    this.pending = next.then(
      () => undefined,
      () => undefined
    );
    return next;
  }

  // connect returns the persistent socket, dialing a new connection if none
  // is currently established.
  private connect(): Promise<net.Socket> {
    if (this.socket && !this.socket.destroyed) {
      return Promise.resolve(this.socket);
    }
    return new Promise((resolve, reject) => {
      const socket = net.createConnection(this.socketPath);
      const onDialError = (err: Error) => {
        socket.destroy();
        reject(new ConnectionError(`Unix socket error: ${err.message}`));
      };
      socket.once('error', onDialError);
      socket.once('connect', () => {
        socket.removeListener('error', onDialError);
        // Tear the connection down on any error or close (e.g. the server's
        // ~30s idle deadline); the next rpc() dials a fresh socket.
        socket.on('error', () => this.teardown(socket));
        socket.on('close', () => this.teardown(socket));
        this.socket = socket;
        this.recvBuffer = '';
        resolve(socket);
      });
    });
  }

  // teardown destroys the given socket and, if it is the active connection,
  // clears it so the next call reconnects lazily.
  private teardown(socket: net.Socket): void {
    socket.destroy();
    if (this.socket === socket) {
      this.socket = null;
      this.recvBuffer = '';
    }
  }

  // dispatch writes a single request to the persistent connection and reads
  // exactly one newline-delimited response, validating the response id.
  private async dispatch(method: string, params: unknown): Promise<unknown> {
    const id = this.nextId++;
    const req: JsonRpcRequest = { jsonrpc: '2.0', method, params, id };
    const line = JSON.stringify(req) + '\n';
    const socket = await this.connect();

    return new Promise((resolve, reject) => {
      let settled = false;

      const cleanup = () => {
        clearTimeout(timer);
        socket.removeListener('data', onData);
        socket.removeListener('error', onError);
        socket.removeListener('close', onClose);
      };

      // fail rejects and destroys the connection so the next call reconnects.
      const fail = (err: Error) => {
        if (settled) return;
        settled = true;
        cleanup();
        this.teardown(socket);
        reject(err);
      };

      // settle finishes the call without tearing down the (healthy) connection.
      const settle = (fn: () => void) => {
        if (settled) return;
        settled = true;
        cleanup();
        fn();
      };

      const timer = setTimeout(() => {
        fail(new ConnectionError(`Unix RPC timeout after ${this.timeout}ms`));
      }, this.timeout);

      const onData = (chunk: Buffer) => {
        this.recvBuffer += chunk.toString('utf8');
        const newlineIdx = this.recvBuffer.indexOf('\n');
        if (newlineIdx === -1) return;

        const jsonLine = this.recvBuffer.substring(0, newlineIdx);
        this.recvBuffer = this.recvBuffer.substring(newlineIdx + 1);

        let resp: JsonRpcResponse;
        try {
          resp = JSON.parse(jsonLine) as JsonRpcResponse;
        } catch (e) {
          fail(new ConnectionError(`Unix RPC parse error: ${(e as Error).message}`));
          return;
        }

        if (resp.id !== id) {
          fail(
            new ConnectionError(`Unix RPC response id mismatch: expected ${id}, got ${resp.id}`)
          );
          return;
        }

        if (resp.error) {
          // Application-level error: the connection itself is still healthy.
          const rpcError = resp.error;
          settle(() => reject(errorFromJsonRpc(rpcError, 'Unix RPC')));
          return;
        }

        settle(() => resolve(resp.result));
      };

      const onError = (err: Error) => {
        fail(new ConnectionError(`Unix socket error: ${err.message}`));
      };
      const onClose = () => {
        fail(new ConnectionError('Unix socket closed before response'));
      };

      socket.on('data', onData);
      socket.on('error', onError);
      socket.on('close', onClose);
      socket.write(line);
    });
  }

  async put(request: PutRequest): Promise<PutResponse> {
    validatePutRequest(request);
    const result = (await this.rpc('put', {
      key: request.key,
      data: request.data.toString('base64'),
      metadata: request.metadata
        ? {
            content_type: request.metadata.contentType,
            content_encoding: request.metadata.contentEncoding,
            custom: request.metadata.custom,
          }
        : undefined,
    })) as Record<string, unknown>;
    return {
      success: true,
      message: result.message as string | undefined,
    };
  }

  async get(request: GetRequest): Promise<GetResponse> {
    validateGetRequest(request);
    const result = (await this.rpc('get', { key: request.key })) as {
      data: string;
      metadata?: {
        content_type?: string;
        content_encoding?: string;
        custom?: Record<string, string>;
      };
    };
    const data = Buffer.from(result.data, 'base64');
    const meta: Metadata = {};
    if (result.metadata) {
      meta.contentType = result.metadata.content_type;
      meta.contentEncoding = result.metadata.content_encoding;
      meta.custom = result.metadata.custom;
    }
    meta.size = data.length;
    return { data, metadata: meta };
  }

  async delete(request: DeleteRequest): Promise<DeleteResponse> {
    validateDeleteRequest(request);
    await this.rpc('delete', { key: request.key });
    return { success: true };
  }

  async list(request: ListRequest = {}): Promise<ListResponse> {
    validateListRequest(request);
    const result = (await this.rpc('list', {
      prefix: request.prefix ?? '',
      delimiter: request.delimiter ?? '',
      max_results: request.maxResults ?? 0,
      continue_from: request.continueFrom ?? '',
    })) as {
      objects?: Array<{ key: string; size?: number; last_modified?: string; etag?: string }>;
      next_cursor?: string;
      is_truncated?: boolean;
    };
    return {
      objects: (result.objects ?? []).map((obj) => ({
        key: obj.key,
        metadata: {
          size: obj.size,
          lastModified: obj.last_modified ? new Date(obj.last_modified) : undefined,
          etag: obj.etag,
        },
      })),
      commonPrefixes: [],
      nextToken: result.next_cursor,
      truncated: result.is_truncated ?? false,
    };
  }

  async exists(request: ExistsRequest): Promise<ExistsResponse> {
    validateExistsRequest(request);
    const result = (await this.rpc('exists', { key: request.key })) as { exists: boolean };
    return { exists: result.exists };
  }

  async getMetadata(request: GetMetadataRequest): Promise<MetadataResponse> {
    validateGetMetadataRequest(request);
    const result = (await this.rpc('get_metadata', { key: request.key })) as {
      content_type?: string;
      content_encoding?: string;
      size?: number;
      last_modified?: string;
      etag?: string;
      custom?: Record<string, string>;
    };
    return {
      success: true,
      metadata: {
        contentType: result.content_type,
        contentEncoding: result.content_encoding,
        size: result.size,
        lastModified: result.last_modified ? new Date(result.last_modified) : undefined,
        etag: result.etag,
        custom: result.custom,
      },
    };
  }

  async updateMetadata(request: UpdateMetadataRequest): Promise<UpdateMetadataResponse> {
    validateUpdateMetadataRequest(request);
    await this.rpc('update_metadata', {
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
    const result = (await this.rpc('health', {})) as { status?: string };
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
    await this.rpc('archive', {
      key: request.key,
      destination_type: request.destinationType,
      destination_settings: request.destinationSettings ?? {},
    });
    return { success: true };
  }

  async addPolicy(request: AddPolicyRequest): Promise<AddPolicyResponse> {
    validateAddPolicyRequest(request);
    // The unix server accepts retention_seconds (taking precedence over the
    // legacy after_days field), so second-granular retention is supported.
    await this.rpc('add_policy', {
      id: request.policy.id,
      prefix: request.policy.prefix,
      action: request.policy.action,
      retention_seconds: request.policy.retentionSeconds,
    });
    return { success: true };
  }

  async removePolicy(request: RemovePolicyRequest): Promise<RemovePolicyResponse> {
    validateRemovePolicyRequest(request);
    await this.rpc('remove_policy', { id: request.id });
    return { success: true };
  }

  async getPolicies(_request: GetPoliciesRequest = {}): Promise<GetPoliciesResponse> {
    type WirePolicy = {
      id: string;
      prefix?: string;
      action?: string;
      after_days?: number;
      retention_seconds?: number;
    };
    // The server returns a BARE JSON array as the JSON-RPC result; accept a
    // wrapped { policies: [...] } shape too, defensively.
    const result = await this.rpc('get_policies', {});
    const wirePolicies: WirePolicy[] = Array.isArray(result)
      ? (result as WirePolicy[])
      : ((result as { policies?: WirePolicy[] } | null)?.policies ?? []);
    return {
      success: true,
      policies: wirePolicies.map((p) => ({
        id: p.id,
        prefix: p.prefix ?? '',
        action: p.action ?? '',
        // Prefer second-granular retention; fall back to legacy whole days.
        retentionSeconds: p.retention_seconds ?? (p.after_days ?? 0) * 86400,
      })),
    };
  }

  async applyPolicies(_request: ApplyPoliciesRequest = {}): Promise<ApplyPoliciesResponse> {
    const result = (await this.rpc('apply_policies', {})) as {
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
    await this.rpc('add_replication_policy', {
      id: request.policy.id,
      source_prefix: request.policy.sourcePrefix ?? '',
      destination_type: request.policy.destinationBackend,
      destination: request.policy.destinationSettings ?? {},
      schedule: '',
      enabled: request.policy.enabled,
    });
    return { success: true };
  }

  async removeReplicationPolicy(
    request: RemoveReplicationPolicyRequest
  ): Promise<RemoveReplicationPolicyResponse> {
    validateRemoveReplicationPolicyRequest(request);
    await this.rpc('remove_replication_policy', { id: request.id });
    return { success: true };
  }

  async getReplicationPolicies(
    _request: GetReplicationPoliciesRequest = {}
  ): Promise<GetReplicationPoliciesResponse> {
    type WireReplicationPolicy = {
      id: string;
      source_prefix?: string;
      destination_type?: string;
      destination?: Record<string, string>;
      schedule?: string;
      enabled?: boolean;
    };
    // The server returns a BARE JSON array as the JSON-RPC result; accept a
    // wrapped { policies: [...] } shape too, defensively.
    const result = await this.rpc('get_replication_policies', {});
    const wirePolicies: WireReplicationPolicy[] = Array.isArray(result)
      ? (result as WireReplicationPolicy[])
      : ((result as { policies?: WireReplicationPolicy[] } | null)?.policies ?? []);
    return {
      policies: wirePolicies.map((p) => ({
        id: p.id,
        sourceBackend: '',
        sourceSettings: {},
        sourcePrefix: p.source_prefix ?? '',
        destinationBackend: p.destination_type ?? '',
        destinationSettings: p.destination ?? {},
        checkIntervalSeconds: 0,
        enabled: p.enabled ?? false,
        replicationMode: stringToReplicationMode('transparent'),
      })),
    };
  }

  async getReplicationPolicy(
    request: GetReplicationPolicyRequest
  ): Promise<GetReplicationPolicyResponse> {
    validateGetReplicationPolicyRequest(request);
    const result = (await this.rpc('get_replication_policy', { id: request.id })) as {
      id?: string;
      source_prefix?: string;
      destination_type?: string;
      destination?: Record<string, string>;
      enabled?: boolean;
    };
    return {
      policy: {
        id: result.id ?? request.id,
        sourceBackend: '',
        sourceSettings: {},
        sourcePrefix: result.source_prefix ?? '',
        destinationBackend: result.destination_type ?? '',
        destinationSettings: result.destination ?? {},
        checkIntervalSeconds: 0,
        enabled: result.enabled ?? false,
        replicationMode: stringToReplicationMode('transparent'),
      },
    };
  }

  async triggerReplication(
    request: TriggerReplicationRequest
  ): Promise<TriggerReplicationResponse> {
    const params: Record<string, unknown> = {};
    if (request.policyId) {
      params.id = request.policyId;
    }
    const result = (await this.rpc('trigger_replication', params)) as {
      objects_synced?: number;
      objects_failed?: number;
      bytes_transferred?: number;
      errors?: string[];
    };
    return {
      success: true,
      result: {
        policyId: request.policyId ?? '',
        synced: result.objects_synced ?? 0,
        deleted: 0,
        failed: result.objects_failed ?? 0,
        bytesTotal: result.bytes_transferred ?? 0,
        durationMs: 0,
        errors: result.errors ?? [],
      },
    };
  }

  async getReplicationStatus(
    request: GetReplicationStatusRequest
  ): Promise<GetReplicationStatusResponse> {
    validateGetReplicationStatusRequest(request);
    const result = (await this.rpc('get_replication_status', { id: request.id })) as {
      policy_id?: string;
      status?: string;
      last_sync_time?: string;
      objects_synced?: number;
      objects_pending?: number;
      objects_failed?: number;
    };
    return {
      success: true,
      status: {
        policyId: result.policy_id ?? request.id,
        sourceBackend: '',
        destinationBackend: '',
        enabled: true,
        totalObjectsSynced: result.objects_synced ?? 0,
        totalObjectsDeleted: 0,
        totalBytesSynced: 0,
        totalErrors: result.objects_failed ?? 0,
        lastSyncTime: result.last_sync_time ? new Date(result.last_sync_time) : undefined,
        averageSyncDurationMs: 0,
        syncCount: 0,
      },
    };
  }

  async close(): Promise<void> {
    if (this.socket) {
      const socket = this.socket;
      this.socket = null;
      this.recvBuffer = '';
      socket.destroy();
    }
    return Promise.resolve();
  }

  // Exposed for test/introspection only.
  get _socketPath(): string {
    return this.socketPath;
  }
}

// Re-export the ReplicationMode helper so callers can import from one place.
export { replicationModeToString, stringToReplicationMode };
