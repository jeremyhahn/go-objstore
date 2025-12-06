/**
 * Input validation utilities for the go-objstore TypeScript SDK
 */

import { ValidationError } from './errors';
import {
  PutRequest,
  GetRequest,
  DeleteRequest,
  ListRequest,
  ExistsRequest,
  GetMetadataRequest,
  UpdateMetadataRequest,
  ArchiveRequest,
  AddPolicyRequest,
  RemovePolicyRequest,
  AddReplicationPolicyRequest,
  RemoveReplicationPolicyRequest,
  GetReplicationPolicyRequest,
  GetReplicationStatusRequest,
} from './types';

/**
 * Validates that a key is a non-empty string
 */
export function validateKey(key: string, fieldName: string = 'key'): void {
  if (typeof key !== 'string') {
    throw new ValidationError(`${fieldName} must be a string`);
  }
  if (key.trim().length === 0) {
    throw new ValidationError(`${fieldName} must not be empty`);
  }
}

/**
 * Validates that data is a Buffer or Uint8Array
 */
export function validateData(data: unknown): asserts data is Buffer {
  if (!Buffer.isBuffer(data) && !(data instanceof Uint8Array)) {
    throw new ValidationError('data must be a Buffer or Uint8Array');
  }
}

/**
 * Validates that a number is positive
 */
export function validatePositiveNumber(value: number, fieldName: string): void {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    throw new ValidationError(`${fieldName} must be a finite number`);
  }
  if (value < 0) {
    throw new ValidationError(`${fieldName} must be a positive number`);
  }
}

/**
 * Validates a PutRequest
 */
export function validatePutRequest(request: PutRequest): void {
  if (!request || typeof request !== 'object') {
    throw new ValidationError('request must be an object');
  }
  validateKey(request.key);
  validateData(request.data);
}

/**
 * Validates a GetRequest
 */
export function validateGetRequest(request: GetRequest): void {
  if (!request || typeof request !== 'object') {
    throw new ValidationError('request must be an object');
  }
  validateKey(request.key);
}

/**
 * Validates a DeleteRequest
 */
export function validateDeleteRequest(request: DeleteRequest): void {
  if (!request || typeof request !== 'object') {
    throw new ValidationError('request must be an object');
  }
  validateKey(request.key);
}

/**
 * Validates a ListRequest
 */
export function validateListRequest(request: ListRequest): void {
  if (!request || typeof request !== 'object') {
    throw new ValidationError('request must be an object');
  }
  if (request.prefix !== undefined) {
    if (typeof request.prefix !== 'string') {
      throw new ValidationError('prefix must be a string');
    }
  }
  if (request.delimiter !== undefined) {
    if (typeof request.delimiter !== 'string') {
      throw new ValidationError('delimiter must be a string');
    }
  }
  if (request.maxResults !== undefined) {
    validatePositiveNumber(request.maxResults, 'maxResults');
    if (request.maxResults === 0) {
      throw new ValidationError('maxResults must be greater than 0');
    }
  }
  if (request.continueFrom !== undefined) {
    if (typeof request.continueFrom !== 'string') {
      throw new ValidationError('continueFrom must be a string');
    }
  }
}

/**
 * Validates an ExistsRequest
 */
export function validateExistsRequest(request: ExistsRequest): void {
  if (!request || typeof request !== 'object') {
    throw new ValidationError('request must be an object');
  }
  validateKey(request.key);
}

/**
 * Validates a GetMetadataRequest
 */
export function validateGetMetadataRequest(request: GetMetadataRequest): void {
  if (!request || typeof request !== 'object') {
    throw new ValidationError('request must be an object');
  }
  validateKey(request.key);
}

/**
 * Validates an UpdateMetadataRequest
 */
export function validateUpdateMetadataRequest(request: UpdateMetadataRequest): void {
  if (!request || typeof request !== 'object') {
    throw new ValidationError('request must be an object');
  }
  validateKey(request.key);
  if (!request.metadata || typeof request.metadata !== 'object') {
    throw new ValidationError('metadata must be an object');
  }
}

/**
 * Validates an ArchiveRequest
 */
export function validateArchiveRequest(request: ArchiveRequest): void {
  if (!request || typeof request !== 'object') {
    throw new ValidationError('request must be an object');
  }
  validateKey(request.key);
  if (typeof request.destinationType !== 'string' || request.destinationType.trim().length === 0) {
    throw new ValidationError('destinationType must be a non-empty string');
  }
}

/**
 * Validates an AddPolicyRequest
 */
export function validateAddPolicyRequest(request: AddPolicyRequest): void {
  if (!request || typeof request !== 'object') {
    throw new ValidationError('request must be an object');
  }
  if (!request.policy || typeof request.policy !== 'object') {
    throw new ValidationError('policy must be an object');
  }
  const { policy } = request;
  validateKey(policy.id, 'policy.id');
  if (typeof policy.prefix !== 'string') {
    throw new ValidationError('policy.prefix must be a string');
  }
  validatePositiveNumber(policy.retentionSeconds, 'policy.retentionSeconds');
  if (typeof policy.action !== 'string' || policy.action.trim().length === 0) {
    throw new ValidationError('policy.action must be a non-empty string');
  }
}

/**
 * Validates a RemovePolicyRequest
 */
export function validateRemovePolicyRequest(request: RemovePolicyRequest): void {
  if (!request || typeof request !== 'object') {
    throw new ValidationError('request must be an object');
  }
  validateKey(request.id, 'id');
}

/**
 * Validates an AddReplicationPolicyRequest
 */
export function validateAddReplicationPolicyRequest(request: AddReplicationPolicyRequest): void {
  if (!request || typeof request !== 'object') {
    throw new ValidationError('request must be an object');
  }
  if (!request.policy || typeof request.policy !== 'object') {
    throw new ValidationError('policy must be an object');
  }
  const { policy } = request;
  validateKey(policy.id, 'policy.id');
  if (typeof policy.sourceBackend !== 'string' || policy.sourceBackend.trim().length === 0) {
    throw new ValidationError('policy.sourceBackend must be a non-empty string');
  }
  if (typeof policy.destinationBackend !== 'string' || policy.destinationBackend.trim().length === 0) {
    throw new ValidationError('policy.destinationBackend must be a non-empty string');
  }
  if (!policy.sourceSettings || typeof policy.sourceSettings !== 'object') {
    throw new ValidationError('policy.sourceSettings must be an object');
  }
  if (!policy.destinationSettings || typeof policy.destinationSettings !== 'object') {
    throw new ValidationError('policy.destinationSettings must be an object');
  }
  validatePositiveNumber(policy.checkIntervalSeconds, 'policy.checkIntervalSeconds');
  if (typeof policy.enabled !== 'boolean') {
    throw new ValidationError('policy.enabled must be a boolean');
  }
}

/**
 * Validates a RemoveReplicationPolicyRequest
 */
export function validateRemoveReplicationPolicyRequest(request: RemoveReplicationPolicyRequest): void {
  if (!request || typeof request !== 'object') {
    throw new ValidationError('request must be an object');
  }
  validateKey(request.id, 'id');
}

/**
 * Validates a GetReplicationPolicyRequest
 */
export function validateGetReplicationPolicyRequest(request: GetReplicationPolicyRequest): void {
  if (!request || typeof request !== 'object') {
    throw new ValidationError('request must be an object');
  }
  validateKey(request.id, 'id');
}

/**
 * Validates a GetReplicationStatusRequest
 */
export function validateGetReplicationStatusRequest(request: GetReplicationStatusRequest): void {
  if (!request || typeof request !== 'object') {
    throw new ValidationError('request must be an object');
  }
  validateKey(request.id, 'id');
}
