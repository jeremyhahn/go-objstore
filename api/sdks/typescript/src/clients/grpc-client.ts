import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { join } from 'path';
import {
  GrpcClientConfig,
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
} from '../types';
import {
  GrpcServiceClient,
  LoadedPackageDefinition,
  ProtoMetadata,
  ProtoGetResponse,
  ProtoObjectInfo,
  ProtoPutResponse,
  ProtoDeleteResponse,
  ProtoListResponse,
  ProtoExistsResponse,
  ProtoMetadataResponse,
  ProtoUpdateMetadataResponse,
  ProtoHealthResponse,
  ProtoArchiveResponse,
  ProtoAddPolicyResponse,
  ProtoRemovePolicyResponse,
  ProtoGetPoliciesResponse,
  ProtoApplyPoliciesResponse,
  ProtoAddReplicationPolicyResponse,
  ProtoRemoveReplicationPolicyResponse,
  ProtoGetReplicationPoliciesResponse,
  ProtoGetReplicationPolicyResponse,
  ProtoTriggerReplicationResponse,
  ProtoGetReplicationStatusResponse,
  ProtoReplicationPolicy,
} from '../grpc-types';
import { ConnectionError } from '../errors';
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

export class GrpcClient implements IObjectStoreClient {
  private client: GrpcServiceClient;
  private protoPath: string;

  constructor(config: GrpcClientConfig) {
    // Try multiple paths to find the proto file
    // 1. In Docker container: /app/proto/objstore.proto
    // 2. In local development: ../../../proto/objstore.proto (from dist)
    // 3. In source: ../../proto/objstore.proto (from src)
    const possiblePaths = [
      '/app/proto/objstore.proto',
      join(__dirname, '../../../proto/objstore.proto'),
      join(__dirname, '../../proto/objstore.proto'),
    ];

    this.protoPath = possiblePaths[0]; // Default to Docker path
    for (const path of possiblePaths) {
      try {
        require('fs').accessSync(path);
        this.protoPath = path;
        break;
      } catch {
        // Continue to next path
      }
    }

    const packageDefinition = protoLoader.loadSync(this.protoPath, {
      keepCase: false,
      longs: String,
      enums: String,
      defaults: true,
      oneofs: true,
    });

    const protoDescriptor = grpc.loadPackageDefinition(packageDefinition) as unknown as LoadedPackageDefinition;
    const ObjectStoreService = protoDescriptor.objstore.v1.ObjectStore;

    const credentials = config.secure
      ? config.credentials || grpc.credentials.createSsl()
      : grpc.credentials.createInsecure();

    this.client = new ObjectStoreService(config.address, credentials, config.options || {});
  }

  async put(request: PutRequest): Promise<PutResponse> {
    validatePutRequest(request);
    return new Promise((resolve, reject) => {
      const grpcRequest = {
        key: request.key,
        data: request.data,
        metadata: request.metadata ? this.serializeMetadata(request.metadata) : undefined,
      };

      this.client.put(grpcRequest, (error: grpc.ServiceError | null, response: ProtoPutResponse) => {
        if (error) {
          reject(this.handleGrpcError(error));
          return;
        }

        resolve({
          success: response.success,
          message: response.message,
          etag: response.etag,
        });
      });
    });
  }

  async get(request: GetRequest): Promise<GetResponse> {
    validateGetRequest(request);
    return new Promise((resolve, reject) => {
      const call = this.client.get({ key: request.key });
      const chunks: Buffer[] = [];
      let metadata: Metadata | undefined;

      call.on('data', (response: ProtoGetResponse) => {
        if (response.metadata && !metadata) {
          metadata = this.deserializeMetadata(response.metadata);
        }
        if (response.data) {
          chunks.push(Buffer.from(response.data));
        }
      });

      call.on('end', () => {
        resolve({
          data: Buffer.concat(chunks),
          metadata,
        });
      });

      call.on('error', (error: grpc.ServiceError) => {
        reject(this.handleGrpcError(error));
      });
    });
  }

  async delete(request: DeleteRequest): Promise<DeleteResponse> {
    validateDeleteRequest(request);
    return new Promise((resolve, reject) => {
      this.client.delete({ key: request.key }, (error: grpc.ServiceError | null, response: ProtoDeleteResponse) => {
        if (error) {
          reject(this.handleGrpcError(error));
          return;
        }

        resolve({
          success: response.success,
          message: response.message,
        });
      });
    });
  }

  async list(request: ListRequest = {}): Promise<ListResponse> {
    validateListRequest(request);
    return new Promise((resolve, reject) => {
      const grpcRequest = {
        prefix: request.prefix || '',
        delimiter: request.delimiter || '',
        maxResults: request.maxResults || 0,
        continueFrom: request.continueFrom || '',
      };

      this.client.list(grpcRequest, (error: grpc.ServiceError | null, response: ProtoListResponse) => {
        if (error) {
          reject(this.handleGrpcError(error));
          return;
        }

        resolve({
          objects: response.objects.map((obj: ProtoObjectInfo) => ({
            key: obj.key,
            metadata: obj.metadata ? this.deserializeMetadata(obj.metadata) : undefined,
          })),
          commonPrefixes: response.commonPrefixes || [],
          nextToken: response.nextToken,
          truncated: response.truncated,
        });
      });
    });
  }

  async exists(request: ExistsRequest): Promise<ExistsResponse> {
    validateExistsRequest(request);
    return new Promise((resolve, reject) => {
      this.client.exists({ key: request.key }, (error: grpc.ServiceError | null, response: ProtoExistsResponse) => {
        if (error) {
          reject(this.handleGrpcError(error));
          return;
        }

        resolve({
          exists: response.exists,
        });
      });
    });
  }

  async getMetadata(request: GetMetadataRequest): Promise<MetadataResponse> {
    validateGetMetadataRequest(request);
    return new Promise((resolve, reject) => {
      this.client.getMetadata({ key: request.key }, (error: grpc.ServiceError | null, response: ProtoMetadataResponse) => {
        if (error) {
          reject(this.handleGrpcError(error));
          return;
        }

        resolve({
          metadata: response.metadata ? this.deserializeMetadata(response.metadata) : undefined,
          success: response.success,
          message: response.message,
        });
      });
    });
  }

  async updateMetadata(request: UpdateMetadataRequest): Promise<UpdateMetadataResponse> {
    validateUpdateMetadataRequest(request);
    return new Promise((resolve, reject) => {
      const grpcRequest = {
        key: request.key,
        metadata: this.serializeMetadata(request.metadata),
      };

      this.client.updateMetadata(grpcRequest, (error: grpc.ServiceError | null, response: ProtoUpdateMetadataResponse) => {
        if (error) {
          reject(this.handleGrpcError(error));
          return;
        }

        resolve({
          success: response.success,
          message: response.message,
        });
      });
    });
  }

  async health(request: HealthRequest = {}): Promise<HealthResponse> {
    return new Promise((resolve, reject) => {
      this.client.health({ service: request.service || '' }, (error: grpc.ServiceError | null, response: ProtoHealthResponse) => {
        if (error) {
          reject(this.handleGrpcError(error));
          return;
        }

        resolve({
          status: this.parseHealthStatus(response.status),
          message: response.message,
        });
      });
    });
  }

  async archive(request: ArchiveRequest): Promise<ArchiveResponse> {
    validateArchiveRequest(request);
    return new Promise((resolve, reject) => {
      const grpcRequest = {
        key: request.key,
        destinationType: request.destinationType,
        destinationSettings: request.destinationSettings || {},
      };

      this.client.archive(grpcRequest, (error: grpc.ServiceError | null, response: ProtoArchiveResponse) => {
        if (error) {
          reject(this.handleGrpcError(error));
          return;
        }

        resolve({
          success: response.success,
          message: response.message,
        });
      });
    });
  }

  async addPolicy(request: AddPolicyRequest): Promise<AddPolicyResponse> {
    validateAddPolicyRequest(request);
    return new Promise((resolve, reject) => {
      const grpcRequest = {
        policy: {
          id: request.policy.id,
          prefix: request.policy.prefix,
          retentionSeconds: request.policy.retentionSeconds,
          action: request.policy.action,
          destinationType: request.policy.destinationType || '',
          destinationSettings: request.policy.destinationSettings || {},
        },
      };

      this.client.addPolicy(grpcRequest, (error: grpc.ServiceError | null, response: ProtoAddPolicyResponse) => {
        if (error) {
          reject(this.handleGrpcError(error));
          return;
        }

        resolve({
          success: response.success,
          message: response.message,
        });
      });
    });
  }

  async removePolicy(request: RemovePolicyRequest): Promise<RemovePolicyResponse> {
    validateRemovePolicyRequest(request);
    return new Promise((resolve, reject) => {
      this.client.removePolicy({ id: request.id }, (error: grpc.ServiceError | null, response: ProtoRemovePolicyResponse) => {
        if (error) {
          reject(this.handleGrpcError(error));
          return;
        }

        resolve({
          success: response.success,
          message: response.message,
        });
      });
    });
  }

  async getPolicies(request: GetPoliciesRequest = {}): Promise<GetPoliciesResponse> {
    return new Promise((resolve, reject) => {
      this.client.getPolicies({ prefix: request.prefix || '' }, (error: grpc.ServiceError | null, response: ProtoGetPoliciesResponse) => {
        if (error) {
          reject(this.handleGrpcError(error));
          return;
        }

        resolve({
          policies: response.policies.map((p) => ({
            id: p.id,
            prefix: p.prefix,
            retentionSeconds: p.retentionSeconds,
            action: p.action,
            destinationType: p.destinationType,
            destinationSettings: p.destinationSettings,
          })),
          success: response.success,
          message: response.message,
        });
      });
    });
  }

  async applyPolicies(_request: ApplyPoliciesRequest = {}): Promise<ApplyPoliciesResponse> {
    return new Promise((resolve, reject) => {
      this.client.applyPolicies({}, (error: grpc.ServiceError | null, response: ProtoApplyPoliciesResponse) => {
        if (error) {
          reject(this.handleGrpcError(error));
          return;
        }

        resolve({
          success: response.success,
          policiesCount: response.policiesCount,
          objectsProcessed: response.objectsProcessed,
          message: response.message,
        });
      });
    });
  }

  async addReplicationPolicy(
    request: AddReplicationPolicyRequest
  ): Promise<AddReplicationPolicyResponse> {
    validateAddReplicationPolicyRequest(request);
    return new Promise((resolve, reject) => {
      const grpcRequest = {
        policy: {
          id: request.policy.id,
          sourceBackend: request.policy.sourceBackend,
          sourceSettings: request.policy.sourceSettings,
          sourcePrefix: request.policy.sourcePrefix,
          destinationBackend: request.policy.destinationBackend,
          destinationSettings: request.policy.destinationSettings,
          checkIntervalSeconds: request.policy.checkIntervalSeconds,
          enabled: request.policy.enabled,
          encryption: request.policy.encryption,
          replicationMode: request.policy.replicationMode,
        },
      };

      this.client.addReplicationPolicy(grpcRequest, (error: grpc.ServiceError | null, response: ProtoAddReplicationPolicyResponse) => {
        if (error) {
          reject(this.handleGrpcError(error));
          return;
        }

        resolve({
          success: response.success,
          message: response.message,
        });
      });
    });
  }

  async removeReplicationPolicy(
    request: RemoveReplicationPolicyRequest
  ): Promise<RemoveReplicationPolicyResponse> {
    validateRemoveReplicationPolicyRequest(request);
    return new Promise((resolve, reject) => {
      this.client.removeReplicationPolicy({ id: request.id }, (error: grpc.ServiceError | null, response: ProtoRemoveReplicationPolicyResponse) => {
        if (error) {
          reject(this.handleGrpcError(error));
          return;
        }

        resolve({
          success: response.success,
          message: response.message,
        });
      });
    });
  }

  async getReplicationPolicies(
    _request: GetReplicationPoliciesRequest = {}
  ): Promise<GetReplicationPoliciesResponse> {
    return new Promise((resolve, reject) => {
      this.client.getReplicationPolicies({}, (error: grpc.ServiceError | null, response: ProtoGetReplicationPoliciesResponse) => {
        if (error) {
          reject(this.handleGrpcError(error));
          return;
        }

        resolve({
          policies: response.policies.map((p: ProtoReplicationPolicy) => ({
            id: p.id,
            sourceBackend: p.sourceBackend,
            sourceSettings: p.sourceSettings,
            sourcePrefix: p.sourcePrefix,
            destinationBackend: p.destinationBackend,
            destinationSettings: p.destinationSettings,
            checkIntervalSeconds: p.checkIntervalSeconds,
            lastSyncTime: p.lastSyncTime ? new Date(p.lastSyncTime.seconds * 1000) : undefined,
            enabled: p.enabled,
            encryption: p.encryption,
            replicationMode: p.replicationMode,
          })),
        });
      });
    });
  }

  async getReplicationPolicy(
    request: GetReplicationPolicyRequest
  ): Promise<GetReplicationPolicyResponse> {
    validateGetReplicationPolicyRequest(request);
    return new Promise((resolve, reject) => {
      this.client.getReplicationPolicy({ id: request.id }, (error: grpc.ServiceError | null, response: ProtoGetReplicationPolicyResponse) => {
        if (error) {
          reject(this.handleGrpcError(error));
          return;
        }

        const p = response.policy;
        resolve({
          policy: {
            id: p.id,
            sourceBackend: p.sourceBackend,
            sourceSettings: p.sourceSettings,
            sourcePrefix: p.sourcePrefix,
            destinationBackend: p.destinationBackend,
            destinationSettings: p.destinationSettings,
            checkIntervalSeconds: p.checkIntervalSeconds,
            lastSyncTime: p.lastSyncTime ? new Date(p.lastSyncTime.seconds * 1000) : undefined,
            enabled: p.enabled,
            encryption: p.encryption,
            replicationMode: p.replicationMode,
          },
        });
      });
    });
  }

  async triggerReplication(
    request: TriggerReplicationRequest
  ): Promise<TriggerReplicationResponse> {
    return new Promise((resolve, reject) => {
      const grpcRequest = {
        policyId: request.policyId || '',
        parallel: request.parallel || false,
        workerCount: request.workerCount || 0,
      };

      this.client.triggerReplication(grpcRequest, (error: grpc.ServiceError | null, response: ProtoTriggerReplicationResponse) => {
        if (error) {
          reject(this.handleGrpcError(error));
          return;
        }

        resolve({
          success: response.success,
          result: response.result
            ? {
                policyId: response.result.policyId,
                synced: response.result.synced,
                deleted: response.result.deleted,
                failed: response.result.failed,
                bytesTotal: response.result.bytesTotal,
                durationMs: response.result.durationMs,
                errors: response.result.errors || [],
              }
            : undefined,
          message: response.message,
        });
      });
    });
  }

  async getReplicationStatus(
    request: GetReplicationStatusRequest
  ): Promise<GetReplicationStatusResponse> {
    validateGetReplicationStatusRequest(request);
    return new Promise((resolve, reject) => {
      this.client.getReplicationStatus({ id: request.id }, (error: grpc.ServiceError | null, response: ProtoGetReplicationStatusResponse) => {
        if (error) {
          reject(this.handleGrpcError(error));
          return;
        }

        resolve({
          success: response.success,
          status: response.status
            ? {
                policyId: response.status.policyId,
                sourceBackend: response.status.sourceBackend,
                destinationBackend: response.status.destinationBackend,
                enabled: response.status.enabled,
                totalObjectsSynced: response.status.totalObjectsSynced,
                totalObjectsDeleted: response.status.totalObjectsDeleted,
                totalBytesSynced: response.status.totalBytesSynced,
                totalErrors: response.status.totalErrors,
                lastSyncTime: response.status.lastSyncTime
                  ? new Date(response.status.lastSyncTime.seconds * 1000)
                  : undefined,
                averageSyncDurationMs: response.status.averageSyncDurationMs,
                syncCount: response.status.syncCount,
              }
            : undefined,
          message: response.message,
        });
      });
    });
  }

  async close(): Promise<void> {
    return new Promise((resolve) => {
      this.client.close();
      resolve();
    });
  }

  private serializeMetadata(metadata: Metadata): ProtoMetadata {
    return {
      contentType: metadata.contentType || '',
      contentEncoding: metadata.contentEncoding || '',
      size: metadata.size || 0,
      lastModified: metadata.lastModified
        ? { seconds: Math.floor(metadata.lastModified.getTime() / 1000), nanos: 0 }
        : undefined,
      etag: metadata.etag || '',
      custom: metadata.custom || {},
    };
  }

  private deserializeMetadata(metadata: ProtoMetadata): Metadata {
    return {
      contentType: metadata.contentType,
      contentEncoding: metadata.contentEncoding,
      size: typeof metadata.size === 'string' ? parseInt(metadata.size, 10) : metadata.size,
      lastModified: metadata.lastModified
        ? new Date(metadata.lastModified.seconds * 1000)
        : undefined,
      etag: metadata.etag,
      custom: metadata.custom,
    };
  }

  private parseHealthStatus(status: string | number): HealthStatus {
    if (typeof status === 'number') {
      return status as HealthStatus;
    }
    // Map string values to enum
    switch (status) {
      case 'SERVING':
        return HealthStatus.SERVING;
      case 'NOT_SERVING':
        return HealthStatus.NOT_SERVING;
      default:
        return HealthStatus.UNKNOWN;
    }
  }

  private handleGrpcError(error: grpc.ServiceError): Error {
    const statusCode = error.code;
    const message = error.details || error.message;

    // Map gRPC status codes to custom errors
    // Use numeric values instead of grpc.status for test compatibility
    switch (statusCode) {
      case 5: // NOT_FOUND
        return new ConnectionError(`Not found: ${message}`, 404);
      case 16: // UNAUTHENTICATED
        return new ConnectionError(`Authentication failed: ${message}`, 401);
      case 7: // PERMISSION_DENIED
        return new ConnectionError(`Permission denied: ${message}`, 403);
      case 14: // UNAVAILABLE
        return new ConnectionError(`Service unavailable: ${message}`, 503);
      case 4: // DEADLINE_EXCEEDED
        return new ConnectionError(`Timeout: ${message}`, 408);
      default:
        return new ConnectionError(`gRPC error (${statusCode}): ${message}`, 500);
    }
  }
}
