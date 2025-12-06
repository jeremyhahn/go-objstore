/**
 * @module @go-objstore/client
 * JavaScript SDK for go-objstore - Unified object storage client
 *
 * Supports multiple protocols:
 * - REST/HTTP
 * - gRPC
 * - QUIC/HTTP3
 *
 * @example
 * import { ObjectStoreClient } from '@go-objstore/client';
 *
 * // REST client
 * const client = new ObjectStoreClient({
 *   protocol: 'rest',
 *   baseURL: 'http://localhost:8080'
 * });
 *
 * // gRPC client
 * const grpcClient = new ObjectStoreClient({
 *   protocol: 'grpc',
 *   baseURL: 'localhost:50051'
 * });
 *
 * // Upload an object
 * await client.put('test.txt', Buffer.from('Hello World'));
 *
 * // Download an object
 * const result = await client.get('test.txt');
 * console.log(result.data.toString());
 *
 * // Clean up
 * client.close();
 */

export { ObjectStoreClient } from './ObjectStoreClient.js';
export { RestClient } from './clients/RestClient.js';
export { GrpcClient } from './clients/GrpcClient.js';
export { QuicClient } from './clients/QuicClient.js';

// Version
export const VERSION = '0.1.0';
