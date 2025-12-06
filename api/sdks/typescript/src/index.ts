/**
 * go-objstore TypeScript SDK
 * Provides unified access to object storage via REST, gRPC, and QUIC/HTTP3 protocols
 */

export { ObjectStoreClient } from './client';
export { RestClient } from './clients/rest-client';
export { GrpcClient } from './clients/grpc-client';
export { QuicClient } from './clients/quic-client';

export * from './types';
export * from './errors';
