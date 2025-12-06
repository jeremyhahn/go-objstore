import { RestClient } from './clients/rest-client';
import { GrpcClient } from './clients/grpc-client';
import { QuicClient } from './clients/quic-client';
import {
  ObjectStoreClientConfig,
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
} from './types';

/**
 * Unified ObjectStore client that provides access to all three protocols:
 * REST, gRPC, and QUIC/HTTP3
 */
export class ObjectStoreClient implements IObjectStoreClient {
  private client: IObjectStoreClient;

  constructor(config: ObjectStoreClientConfig) {
    switch (config.protocol) {
      case 'rest':
        if (!config.rest) {
          throw new Error('REST configuration is required when using REST protocol');
        }
        this.client = new RestClient(config.rest);
        break;

      case 'grpc':
        if (!config.grpc) {
          throw new Error('gRPC configuration is required when using gRPC protocol');
        }
        this.client = new GrpcClient(config.grpc);
        break;

      case 'quic':
        if (!config.quic) {
          throw new Error('QUIC configuration is required when using QUIC protocol');
        }
        this.client = new QuicClient(config.quic);
        break;

      default:
        throw new Error(`Unsupported protocol: ${config.protocol}`);
    }
  }

  /**
   * Store an object in the backend
   */
  async put(request: PutRequest): Promise<PutResponse> {
    return this.client.put(request);
  }

  /**
   * Retrieve an object from the backend
   */
  async get(request: GetRequest): Promise<GetResponse> {
    return this.client.get(request);
  }

  /**
   * Delete an object from the backend
   */
  async delete(request: DeleteRequest): Promise<DeleteResponse> {
    return this.client.delete(request);
  }

  /**
   * List objects with optional filtering and pagination
   */
  async list(request: ListRequest = {}): Promise<ListResponse> {
    return this.client.list(request);
  }

  /**
   * Check if an object exists
   */
  async exists(request: ExistsRequest): Promise<ExistsResponse> {
    return this.client.exists(request);
  }

  /**
   * Get metadata for an object without retrieving its content
   */
  async getMetadata(request: GetMetadataRequest): Promise<MetadataResponse> {
    return this.client.getMetadata(request);
  }

  /**
   * Update metadata for an existing object
   */
  async updateMetadata(request: UpdateMetadataRequest): Promise<UpdateMetadataResponse> {
    return this.client.updateMetadata(request);
  }

  /**
   * Check service health
   */
  async health(request: HealthRequest = {}): Promise<HealthResponse> {
    return this.client.health(request);
  }

  /**
   * Archive an object to a different storage backend
   */
  async archive(request: ArchiveRequest): Promise<ArchiveResponse> {
    return this.client.archive(request);
  }

  /**
   * Add a lifecycle policy
   */
  async addPolicy(request: AddPolicyRequest): Promise<AddPolicyResponse> {
    return this.client.addPolicy(request);
  }

  /**
   * Remove a lifecycle policy
   */
  async removePolicy(request: RemovePolicyRequest): Promise<RemovePolicyResponse> {
    return this.client.removePolicy(request);
  }

  /**
   * Get all lifecycle policies
   */
  async getPolicies(request: GetPoliciesRequest = {}): Promise<GetPoliciesResponse> {
    return this.client.getPolicies(request);
  }

  /**
   * Apply all lifecycle policies
   */
  async applyPolicies(request: ApplyPoliciesRequest = {}): Promise<ApplyPoliciesResponse> {
    return this.client.applyPolicies(request);
  }

  /**
   * Add a replication policy
   */
  async addReplicationPolicy(
    request: AddReplicationPolicyRequest
  ): Promise<AddReplicationPolicyResponse> {
    return this.client.addReplicationPolicy(request);
  }

  /**
   * Remove a replication policy
   */
  async removeReplicationPolicy(
    request: RemoveReplicationPolicyRequest
  ): Promise<RemoveReplicationPolicyResponse> {
    return this.client.removeReplicationPolicy(request);
  }

  /**
   * Get all replication policies
   */
  async getReplicationPolicies(
    request: GetReplicationPoliciesRequest = {}
  ): Promise<GetReplicationPoliciesResponse> {
    return this.client.getReplicationPolicies(request);
  }

  /**
   * Get a specific replication policy
   */
  async getReplicationPolicy(
    request: GetReplicationPolicyRequest
  ): Promise<GetReplicationPolicyResponse> {
    return this.client.getReplicationPolicy(request);
  }

  /**
   * Trigger replication synchronization
   */
  async triggerReplication(
    request: TriggerReplicationRequest
  ): Promise<TriggerReplicationResponse> {
    return this.client.triggerReplication(request);
  }

  /**
   * Get replication status and metrics
   */
  async getReplicationStatus(
    request: GetReplicationStatusRequest
  ): Promise<GetReplicationStatusResponse> {
    return this.client.getReplicationStatus(request);
  }

  /**
   * Close the client and cleanup resources
   */
  async close(): Promise<void> {
    return this.client.close();
  }
}
