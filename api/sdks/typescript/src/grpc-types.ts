/**
 * Type definitions for gRPC client
 * These types represent the protobuf message structures
 */

import * as grpc from '@grpc/grpc-js';

/**
 * Timestamp representation from protobuf
 */
export interface ProtoTimestamp {
  seconds: number;
  nanos: number;
}

/**
 * Metadata message from protobuf
 */
export interface ProtoMetadata {
  contentType: string;
  contentEncoding: string;
  size: number;
  lastModified?: ProtoTimestamp;
  etag: string;
  custom: Record<string, string>;
}

/**
 * ObjectInfo message from protobuf
 */
export interface ProtoObjectInfo {
  key: string;
  metadata?: ProtoMetadata;
}

/**
 * PutRequest message for gRPC
 */
export interface ProtoPutRequest {
  key: string;
  data: Buffer | Uint8Array;
  metadata?: ProtoMetadata;
}

/**
 * PutResponse message from gRPC
 */
export interface ProtoPutResponse {
  success: boolean;
  message: string;
  etag: string;
}

/**
 * GetRequest message for gRPC
 */
export interface ProtoGetRequest {
  key: string;
}

/**
 * GetResponse message from gRPC (streaming)
 */
export interface ProtoGetResponse {
  data?: Buffer | Uint8Array;
  metadata?: ProtoMetadata;
}

/**
 * DeleteRequest message for gRPC
 */
export interface ProtoDeleteRequest {
  key: string;
}

/**
 * DeleteResponse message from gRPC
 */
export interface ProtoDeleteResponse {
  success: boolean;
  message: string;
}

/**
 * ListRequest message for gRPC
 */
export interface ProtoListRequest {
  prefix: string;
  delimiter: string;
  maxResults: number;
  continueFrom: string;
}

/**
 * ListResponse message from gRPC
 */
export interface ProtoListResponse {
  objects: ProtoObjectInfo[];
  commonPrefixes: string[];
  nextToken: string;
  truncated: boolean;
}

/**
 * ExistsRequest message for gRPC
 */
export interface ProtoExistsRequest {
  key: string;
}

/**
 * ExistsResponse message from gRPC
 */
export interface ProtoExistsResponse {
  exists: boolean;
}

/**
 * GetMetadataRequest message for gRPC
 */
export interface ProtoGetMetadataRequest {
  key: string;
}

/**
 * MetadataResponse message from gRPC
 */
export interface ProtoMetadataResponse {
  metadata?: ProtoMetadata;
  success: boolean;
  message: string;
}

/**
 * UpdateMetadataRequest message for gRPC
 */
export interface ProtoUpdateMetadataRequest {
  key: string;
  metadata: ProtoMetadata;
}

/**
 * UpdateMetadataResponse message from gRPC
 */
export interface ProtoUpdateMetadataResponse {
  success: boolean;
  message: string;
}

/**
 * HealthRequest message for gRPC
 */
export interface ProtoHealthRequest {
  service: string;
}

/**
 * HealthResponse message from gRPC
 */
export interface ProtoHealthResponse {
  status: number;
  message: string;
}

/**
 * ArchiveRequest message for gRPC
 */
export interface ProtoArchiveRequest {
  key: string;
  destinationType: string;
  destinationSettings: Record<string, string>;
}

/**
 * ArchiveResponse message from gRPC
 */
export interface ProtoArchiveResponse {
  success: boolean;
  message: string;
}

/**
 * LifecyclePolicy message from protobuf
 */
export interface ProtoLifecyclePolicy {
  id: string;
  prefix: string;
  retentionSeconds: number;
  action: string;
  destinationType: string;
  destinationSettings: Record<string, string>;
}

/**
 * AddPolicyRequest message for gRPC
 */
export interface ProtoAddPolicyRequest {
  policy: ProtoLifecyclePolicy;
}

/**
 * AddPolicyResponse message from gRPC
 */
export interface ProtoAddPolicyResponse {
  success: boolean;
  message: string;
}

/**
 * RemovePolicyRequest message for gRPC
 */
export interface ProtoRemovePolicyRequest {
  id: string;
}

/**
 * RemovePolicyResponse message from gRPC
 */
export interface ProtoRemovePolicyResponse {
  success: boolean;
  message: string;
}

/**
 * GetPoliciesRequest message for gRPC
 */
export interface ProtoGetPoliciesRequest {
  prefix: string;
}

/**
 * GetPoliciesResponse message from gRPC
 */
export interface ProtoGetPoliciesResponse {
  policies: ProtoLifecyclePolicy[];
  success: boolean;
  message: string;
}

/**
 * ApplyPoliciesRequest message for gRPC
 */
export interface ProtoApplyPoliciesRequest {}

/**
 * ApplyPoliciesResponse message from gRPC
 */
export interface ProtoApplyPoliciesResponse {
  success: boolean;
  policiesCount: number;
  objectsProcessed: number;
  message: string;
}

/**
 * EncryptionConfig message from protobuf
 */
export interface ProtoEncryptionConfig {
  enabled: boolean;
  provider: string;
  defaultKey: string;
}

/**
 * EncryptionPolicy message from protobuf
 */
export interface ProtoEncryptionPolicy {
  backend?: ProtoEncryptionConfig;
  source?: ProtoEncryptionConfig;
  destination?: ProtoEncryptionConfig;
}

/**
 * ReplicationPolicy message from protobuf
 */
export interface ProtoReplicationPolicy {
  id: string;
  sourceBackend: string;
  sourceSettings: Record<string, string>;
  sourcePrefix: string;
  destinationBackend: string;
  destinationSettings: Record<string, string>;
  checkIntervalSeconds: number;
  lastSyncTime?: ProtoTimestamp;
  enabled: boolean;
  encryption?: ProtoEncryptionPolicy;
  replicationMode: number;
}

/**
 * AddReplicationPolicyRequest message for gRPC
 */
export interface ProtoAddReplicationPolicyRequest {
  policy: ProtoReplicationPolicy;
}

/**
 * AddReplicationPolicyResponse message from gRPC
 */
export interface ProtoAddReplicationPolicyResponse {
  success: boolean;
  message: string;
}

/**
 * RemoveReplicationPolicyRequest message for gRPC
 */
export interface ProtoRemoveReplicationPolicyRequest {
  id: string;
}

/**
 * RemoveReplicationPolicyResponse message from gRPC
 */
export interface ProtoRemoveReplicationPolicyResponse {
  success: boolean;
  message: string;
}

/**
 * GetReplicationPoliciesRequest message for gRPC
 */
export interface ProtoGetReplicationPoliciesRequest {}

/**
 * GetReplicationPoliciesResponse message from gRPC
 */
export interface ProtoGetReplicationPoliciesResponse {
  policies: ProtoReplicationPolicy[];
}

/**
 * GetReplicationPolicyRequest message for gRPC
 */
export interface ProtoGetReplicationPolicyRequest {
  id: string;
}

/**
 * GetReplicationPolicyResponse message from gRPC
 */
export interface ProtoGetReplicationPolicyResponse {
  policy: ProtoReplicationPolicy;
}

/**
 * SyncResult message from protobuf
 */
export interface ProtoSyncResult {
  policyId: string;
  synced: number;
  deleted: number;
  failed: number;
  bytesTotal: number;
  durationMs: number;
  errors: string[];
}

/**
 * TriggerReplicationRequest message for gRPC
 */
export interface ProtoTriggerReplicationRequest {
  policyId: string;
  parallel: boolean;
  workerCount: number;
}

/**
 * TriggerReplicationResponse message from gRPC
 */
export interface ProtoTriggerReplicationResponse {
  success: boolean;
  result?: ProtoSyncResult;
  message: string;
}

/**
 * GetReplicationStatusRequest message for gRPC
 */
export interface ProtoGetReplicationStatusRequest {
  id: string;
}

/**
 * ReplicationStatus message from protobuf
 */
export interface ProtoReplicationStatus {
  policyId: string;
  sourceBackend: string;
  destinationBackend: string;
  enabled: boolean;
  totalObjectsSynced: number;
  totalObjectsDeleted: number;
  totalBytesSynced: number;
  totalErrors: number;
  lastSyncTime?: ProtoTimestamp;
  averageSyncDurationMs: number;
  syncCount: number;
}

/**
 * GetReplicationStatusResponse message from gRPC
 */
export interface ProtoGetReplicationStatusResponse {
  success: boolean;
  status?: ProtoReplicationStatus;
  message: string;
}

/**
 * gRPC service client interface
 */
export interface GrpcServiceClient {
  put(
    request: ProtoPutRequest,
    callback: (error: grpc.ServiceError | null, response: ProtoPutResponse) => void
  ): void;

  get(request: ProtoGetRequest): grpc.ClientReadableStream<ProtoGetResponse>;

  delete(
    request: ProtoDeleteRequest,
    callback: (error: grpc.ServiceError | null, response: ProtoDeleteResponse) => void
  ): void;

  list(
    request: ProtoListRequest,
    callback: (error: grpc.ServiceError | null, response: ProtoListResponse) => void
  ): void;

  exists(
    request: ProtoExistsRequest,
    callback: (error: grpc.ServiceError | null, response: ProtoExistsResponse) => void
  ): void;

  getMetadata(
    request: ProtoGetMetadataRequest,
    callback: (error: grpc.ServiceError | null, response: ProtoMetadataResponse) => void
  ): void;

  updateMetadata(
    request: ProtoUpdateMetadataRequest,
    callback: (error: grpc.ServiceError | null, response: ProtoUpdateMetadataResponse) => void
  ): void;

  health(
    request: ProtoHealthRequest,
    callback: (error: grpc.ServiceError | null, response: ProtoHealthResponse) => void
  ): void;

  archive(
    request: ProtoArchiveRequest,
    callback: (error: grpc.ServiceError | null, response: ProtoArchiveResponse) => void
  ): void;

  addPolicy(
    request: ProtoAddPolicyRequest,
    callback: (error: grpc.ServiceError | null, response: ProtoAddPolicyResponse) => void
  ): void;

  removePolicy(
    request: ProtoRemovePolicyRequest,
    callback: (error: grpc.ServiceError | null, response: ProtoRemovePolicyResponse) => void
  ): void;

  getPolicies(
    request: ProtoGetPoliciesRequest,
    callback: (error: grpc.ServiceError | null, response: ProtoGetPoliciesResponse) => void
  ): void;

  applyPolicies(
    request: ProtoApplyPoliciesRequest,
    callback: (error: grpc.ServiceError | null, response: ProtoApplyPoliciesResponse) => void
  ): void;

  addReplicationPolicy(
    request: ProtoAddReplicationPolicyRequest,
    callback: (error: grpc.ServiceError | null, response: ProtoAddReplicationPolicyResponse) => void
  ): void;

  removeReplicationPolicy(
    request: ProtoRemoveReplicationPolicyRequest,
    callback: (error: grpc.ServiceError | null, response: ProtoRemoveReplicationPolicyResponse) => void
  ): void;

  getReplicationPolicies(
    request: ProtoGetReplicationPoliciesRequest,
    callback: (error: grpc.ServiceError | null, response: ProtoGetReplicationPoliciesResponse) => void
  ): void;

  getReplicationPolicy(
    request: ProtoGetReplicationPolicyRequest,
    callback: (error: grpc.ServiceError | null, response: ProtoGetReplicationPolicyResponse) => void
  ): void;

  triggerReplication(
    request: ProtoTriggerReplicationRequest,
    callback: (error: grpc.ServiceError | null, response: ProtoTriggerReplicationResponse) => void
  ): void;

  getReplicationStatus(
    request: ProtoGetReplicationStatusRequest,
    callback: (error: grpc.ServiceError | null, response: ProtoGetReplicationStatusResponse) => void
  ): void;

  close(): void;
}

/**
 * Type for the loaded package definition
 */
export interface LoadedPackageDefinition {
  objstore: {
    v1: {
      ObjectStore: new (
        address: string,
        credentials: grpc.ChannelCredentials,
        options: grpc.ClientOptions
      ) => GrpcServiceClient;
    };
  };
}
