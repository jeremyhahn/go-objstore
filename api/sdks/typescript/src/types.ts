/**
 * Type definitions for the go-objstore TypeScript SDK
 */

import * as grpc from '@grpc/grpc-js';

export enum HealthStatus {
  UNKNOWN = 0,
  SERVING = 1,
  NOT_SERVING = 2,
}

export enum ReplicationMode {
  TRANSPARENT = 0,
  OPAQUE = 1,
}

/**
 * Convert ReplicationMode enum to string for REST/QUIC APIs.
 * gRPC uses numeric values, but REST/QUIC APIs expect "transparent" or "opaque".
 */
export function replicationModeToString(mode: ReplicationMode): string {
  switch (mode) {
    case ReplicationMode.TRANSPARENT:
      return 'transparent';
    case ReplicationMode.OPAQUE:
      return 'opaque';
    default:
      return 'transparent';
  }
}

/**
 * Convert string to ReplicationMode enum from REST/QUIC API responses.
 */
export function stringToReplicationMode(mode: string | number): ReplicationMode {
  if (typeof mode === 'number') {
    return mode as ReplicationMode;
  }
  switch (mode?.toLowerCase()) {
    case 'opaque':
      return ReplicationMode.OPAQUE;
    case 'transparent':
    default:
      return ReplicationMode.TRANSPARENT;
  }
}

export interface Metadata {
  contentType?: string;
  contentEncoding?: string;
  size?: number;
  lastModified?: Date;
  etag?: string;
  custom?: Record<string, string>;
}

export interface ObjectInfo {
  key: string;
  metadata?: Metadata;
}

export interface PutRequest {
  key: string;
  data: Buffer;
  metadata?: Metadata;
}

export interface PutResponse {
  success: boolean;
  message?: string;
  etag?: string;
}

export interface GetRequest {
  key: string;
}

export interface GetResponse {
  data: Buffer;
  metadata?: Metadata;
}

export interface DeleteRequest {
  key: string;
}

export interface DeleteResponse {
  success: boolean;
  message?: string;
}

export interface ListRequest {
  prefix?: string;
  delimiter?: string;
  maxResults?: number;
  continueFrom?: string;
}

export interface ListResponse {
  objects: ObjectInfo[];
  commonPrefixes: string[];
  nextToken?: string;
  truncated: boolean;
}

export interface ExistsRequest {
  key: string;
}

export interface ExistsResponse {
  exists: boolean;
}

export interface GetMetadataRequest {
  key: string;
}

export interface MetadataResponse {
  metadata?: Metadata;
  success: boolean;
  message?: string;
}

export interface UpdateMetadataRequest {
  key: string;
  metadata: Metadata;
}

export interface UpdateMetadataResponse {
  success: boolean;
  message?: string;
}

export interface HealthRequest {
  service?: string;
}

export interface HealthResponse {
  status: HealthStatus;
  message?: string;
}

export interface ArchiveRequest {
  key: string;
  destinationType: string;
  destinationSettings?: Record<string, string>;
}

export interface ArchiveResponse {
  success: boolean;
  message?: string;
}

export interface LifecyclePolicy {
  id: string;
  prefix: string;
  retentionSeconds: number;
  action: string;
  destinationType?: string;
  destinationSettings?: Record<string, string>;
}

export interface AddPolicyRequest {
  policy: LifecyclePolicy;
}

export interface AddPolicyResponse {
  success: boolean;
  message?: string;
}

export interface RemovePolicyRequest {
  id: string;
}

export interface RemovePolicyResponse {
  success: boolean;
  message?: string;
}

export interface GetPoliciesRequest {
  prefix?: string;
}

export interface GetPoliciesResponse {
  policies: LifecyclePolicy[];
  success: boolean;
  message?: string;
}

export interface ApplyPoliciesRequest {}

export interface ApplyPoliciesResponse {
  success: boolean;
  policiesCount: number;
  objectsProcessed: number;
  message?: string;
}

export interface EncryptionConfig {
  enabled: boolean;
  provider: string;
  defaultKey: string;
}

export interface EncryptionPolicy {
  backend?: EncryptionConfig;
  source?: EncryptionConfig;
  destination?: EncryptionConfig;
}

export interface ReplicationPolicy {
  id: string;
  sourceBackend: string;
  sourceSettings: Record<string, string>;
  sourcePrefix: string;
  destinationBackend: string;
  destinationSettings: Record<string, string>;
  checkIntervalSeconds: number;
  lastSyncTime?: Date;
  enabled: boolean;
  encryption?: EncryptionPolicy;
  replicationMode: ReplicationMode;
}

export interface AddReplicationPolicyRequest {
  policy: ReplicationPolicy;
}

export interface AddReplicationPolicyResponse {
  success: boolean;
  message?: string;
}

export interface RemoveReplicationPolicyRequest {
  id: string;
}

export interface RemoveReplicationPolicyResponse {
  success: boolean;
  message?: string;
}

export interface GetReplicationPoliciesRequest {}

export interface GetReplicationPoliciesResponse {
  policies: ReplicationPolicy[];
}

export interface GetReplicationPolicyRequest {
  id: string;
}

export interface GetReplicationPolicyResponse {
  policy?: ReplicationPolicy;
}

export interface SyncResult {
  policyId: string;
  synced: number;
  deleted: number;
  failed: number;
  bytesTotal: number;
  durationMs: number;
  errors: string[];
}

export interface TriggerReplicationRequest {
  policyId?: string;
  parallel?: boolean;
  workerCount?: number;
}

export interface TriggerReplicationResponse {
  success: boolean;
  result?: SyncResult;
  message?: string;
}

export interface GetReplicationStatusRequest {
  id: string;
}

export interface ReplicationStatus {
  policyId: string;
  sourceBackend: string;
  destinationBackend: string;
  enabled: boolean;
  totalObjectsSynced: number;
  totalObjectsDeleted: number;
  totalBytesSynced: number;
  totalErrors: number;
  lastSyncTime?: Date;
  averageSyncDurationMs: number;
  syncCount: number;
}

export interface GetReplicationStatusResponse {
  success: boolean;
  status?: ReplicationStatus;
  message?: string;
}

export interface ClientConfig {
  baseUrl: string;
  timeout?: number;
  headers?: Record<string, string>;
}

export interface GrpcClientConfig {
  address: string;
  secure?: boolean;
  credentials?: grpc.ChannelCredentials;
  options?: grpc.ClientOptions;
}

export interface QuicClientConfig {
  address: string;
  secure?: boolean;
  certificates?: {
    ca?: Buffer;
    cert?: Buffer;
    key?: Buffer;
  };
}

export type ProtocolType = 'rest' | 'grpc' | 'quic';

export interface ObjectStoreClientConfig {
  protocol: ProtocolType;
  rest?: ClientConfig;
  grpc?: GrpcClientConfig;
  quic?: QuicClientConfig;
}

export interface IObjectStoreClient {
  // Object operations
  put(request: PutRequest): Promise<PutResponse>;
  get(request: GetRequest): Promise<GetResponse>;
  delete(request: DeleteRequest): Promise<DeleteResponse>;
  list(request: ListRequest): Promise<ListResponse>;
  exists(request: ExistsRequest): Promise<ExistsResponse>;

  // Metadata operations
  getMetadata(request: GetMetadataRequest): Promise<MetadataResponse>;
  updateMetadata(request: UpdateMetadataRequest): Promise<UpdateMetadataResponse>;

  // Health check
  health(request?: HealthRequest): Promise<HealthResponse>;

  // Archive operations
  archive(request: ArchiveRequest): Promise<ArchiveResponse>;

  // Lifecycle policy operations
  addPolicy(request: AddPolicyRequest): Promise<AddPolicyResponse>;
  removePolicy(request: RemovePolicyRequest): Promise<RemovePolicyResponse>;
  getPolicies(request?: GetPoliciesRequest): Promise<GetPoliciesResponse>;
  applyPolicies(request?: ApplyPoliciesRequest): Promise<ApplyPoliciesResponse>;

  // Replication policy operations
  addReplicationPolicy(request: AddReplicationPolicyRequest): Promise<AddReplicationPolicyResponse>;
  removeReplicationPolicy(
    request: RemoveReplicationPolicyRequest
  ): Promise<RemoveReplicationPolicyResponse>;
  getReplicationPolicies(
    request?: GetReplicationPoliciesRequest
  ): Promise<GetReplicationPoliciesResponse>;
  getReplicationPolicy(request: GetReplicationPolicyRequest): Promise<GetReplicationPolicyResponse>;
  triggerReplication(request: TriggerReplicationRequest): Promise<TriggerReplicationResponse>;
  getReplicationStatus(request: GetReplicationStatusRequest): Promise<GetReplicationStatusResponse>;

  // Cleanup
  close(): Promise<void>;
}
