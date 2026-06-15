/**
 * Shared JSON-RPC 2.0 envelope types for the MCP and Unix-socket clients,
 * matching the server's shared envelope in pkg/server/jsonrpc.
 */

import {
  ObjectStoreError,
  ObjectNotFoundError,
  AuthenticationError,
  AuthorizationError,
  ValidationError,
  AlreadyExistsError,
  RateLimitError,
  ServerError,
} from '../errors';

/** A JSON-RPC 2.0 request envelope. */
export interface JsonRpcRequest<P = unknown> {
  jsonrpc: '2.0';
  method: string;
  params: P;
  id: number;
}

/** A JSON-RPC 2.0 error object. */
export interface JsonRpcError {
  code: number;
  message: string;
  data?: unknown;
}

/** A JSON-RPC 2.0 response envelope. */
export interface JsonRpcResponse<R = unknown> {
  jsonrpc: '2.0';
  result?: R;
  error?: JsonRpcError;
  id: number;
}

/** Implementation-defined JSON-RPC error codes used by the objstore server. */
export const JSON_RPC_FORBIDDEN = -32001;
export const JSON_RPC_UNAUTHENTICATED = -32002;
export const JSON_RPC_NOT_FOUND = -32004;
export const JSON_RPC_ALREADY_EXISTS = -32005;
export const JSON_RPC_RATE_LIMITED = -32029;
/** Standard JSON-RPC 2.0 invalid-params error code. */
export const JSON_RPC_INVALID_PARAMS = -32602;

/**
 * errorFromJsonRpc converts a JSON-RPC error object from the objstore server
 * into the matching typed SDK error. The transport context (e.g. "Unix RPC",
 * "MCP") and the server's code and message are preserved in the error message.
 */
export function errorFromJsonRpc(error: JsonRpcError, context: string): ObjectStoreError {
  const detail = `${context} error (${error.code}): ${error.message}`;
  switch (error.code) {
    case JSON_RPC_NOT_FOUND:
      return new ObjectNotFoundError(detail);
    case JSON_RPC_UNAUTHENTICATED:
      return new AuthenticationError(detail);
    case JSON_RPC_FORBIDDEN:
      return new AuthorizationError(detail);
    case JSON_RPC_INVALID_PARAMS:
      return new ValidationError(detail);
    case JSON_RPC_ALREADY_EXISTS:
      return new AlreadyExistsError(detail);
    case JSON_RPC_RATE_LIMITED:
      return new RateLimitError(detail);
    default:
      return new ServerError(detail);
  }
}
