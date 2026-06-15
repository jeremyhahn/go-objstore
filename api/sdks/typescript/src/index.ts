/**
 * go-objstore TypeScript SDK
 * Provides unified access to object storage via REST, gRPC, QUIC/HTTP3,
 * MCP (Model Context Protocol), and Unix domain socket protocols.
 */

export { ObjectStoreClient } from './client';
export { RestClient } from './clients/rest-client';
export { GrpcClient } from './clients/grpc-client';
export { QuicClient } from './clients/quic-client';
export { McpClient } from './clients/mcp-client';
export { UnixClient } from './clients/unix-client';

export * from './types';
export * from './errors';
