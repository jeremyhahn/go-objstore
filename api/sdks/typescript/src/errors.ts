/**
 * Error classes for the go-objstore TypeScript SDK
 */

/**
 * Base error class for all ObjectStore errors
 */
export class ObjectStoreError extends Error {
  public readonly code: string;
  public readonly statusCode?: number;

  constructor(message: string, code: string, statusCode?: number) {
    super(message);
    this.name = this.constructor.name;
    this.code = code;
    this.statusCode = statusCode;
    Error.captureStackTrace(this, this.constructor);
  }
}

/**
 * Error thrown when an object is not found
 */
export class ObjectNotFoundError extends ObjectStoreError {
  constructor(key: string) {
    super(`Object not found: ${key}`, 'OBJECT_NOT_FOUND', 404);
  }
}

/**
 * Error thrown when connection to the backend fails
 */
export class ConnectionError extends ObjectStoreError {
  constructor(message: string, statusCode?: number) {
    super(message, 'CONNECTION_ERROR', statusCode);
  }
}

/**
 * Error thrown when a policy is not found
 */
export class PolicyNotFoundError extends ObjectStoreError {
  constructor(policyId: string) {
    super(`Policy not found: ${policyId}`, 'POLICY_NOT_FOUND', 404);
  }
}

/**
 * Error thrown when input validation fails
 */
export class ValidationError extends ObjectStoreError {
  constructor(message: string) {
    super(message, 'VALIDATION_ERROR', 400);
  }
}

/**
 * Error thrown when authentication fails
 */
export class AuthenticationError extends ObjectStoreError {
  constructor(message: string) {
    super(message, 'AUTHENTICATION_ERROR', 401);
  }
}

/**
 * Error thrown when authorization fails
 */
export class AuthorizationError extends ObjectStoreError {
  constructor(message: string) {
    super(message, 'AUTHORIZATION_ERROR', 403);
  }
}

/**
 * Error thrown when a server error occurs
 */
export class ServerError extends ObjectStoreError {
  constructor(message: string, statusCode: number = 500) {
    super(message, 'SERVER_ERROR', statusCode);
  }
}

/**
 * Error thrown when a timeout occurs
 */
export class TimeoutError extends ObjectStoreError {
  constructor(message: string) {
    super(message, 'TIMEOUT_ERROR', 408);
  }
}
