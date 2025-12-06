import {
  ObjectStoreError,
  ObjectNotFoundError,
  ConnectionError,
  PolicyNotFoundError,
  ValidationError,
  AuthenticationError,
  AuthorizationError,
  ServerError,
  TimeoutError,
} from '../../src/errors';

describe('Error Classes', () => {
  describe('ObjectStoreError', () => {
    it('should create error with message, code, and statusCode', () => {
      const error = new ObjectStoreError('Test error', 'TEST_ERROR', 500);
      expect(error.message).toBe('Test error');
      expect(error.code).toBe('TEST_ERROR');
      expect(error.statusCode).toBe(500);
      expect(error.name).toBe('ObjectStoreError');
      expect(error).toBeInstanceOf(Error);
    });

    it('should create error without statusCode', () => {
      const error = new ObjectStoreError('Test error', 'TEST_ERROR');
      expect(error.statusCode).toBeUndefined();
    });
  });

  describe('ObjectNotFoundError', () => {
    it('should create error with object key', () => {
      const error = new ObjectNotFoundError('test/file.txt');
      expect(error.message).toBe('Object not found: test/file.txt');
      expect(error.code).toBe('OBJECT_NOT_FOUND');
      expect(error.statusCode).toBe(404);
    });
  });

  describe('ConnectionError', () => {
    it('should create error with message', () => {
      const error = new ConnectionError('Connection failed');
      expect(error.message).toBe('Connection failed');
      expect(error.code).toBe('CONNECTION_ERROR');
    });

    it('should create error with statusCode', () => {
      const error = new ConnectionError('Connection timeout', 408);
      expect(error.statusCode).toBe(408);
    });
  });

  describe('PolicyNotFoundError', () => {
    it('should create error with policy ID', () => {
      const error = new PolicyNotFoundError('policy-123');
      expect(error.message).toBe('Policy not found: policy-123');
      expect(error.code).toBe('POLICY_NOT_FOUND');
      expect(error.statusCode).toBe(404);
    });
  });

  describe('ValidationError', () => {
    it('should create error with validation message', () => {
      const error = new ValidationError('Invalid key format');
      expect(error.message).toBe('Invalid key format');
      expect(error.code).toBe('VALIDATION_ERROR');
      expect(error.statusCode).toBe(400);
    });
  });

  describe('AuthenticationError', () => {
    it('should create error with auth message', () => {
      const error = new AuthenticationError('Invalid credentials');
      expect(error.message).toBe('Invalid credentials');
      expect(error.code).toBe('AUTHENTICATION_ERROR');
      expect(error.statusCode).toBe(401);
    });
  });

  describe('AuthorizationError', () => {
    it('should create error with authorization message', () => {
      const error = new AuthorizationError('Access denied');
      expect(error.message).toBe('Access denied');
      expect(error.code).toBe('AUTHORIZATION_ERROR');
      expect(error.statusCode).toBe(403);
    });
  });

  describe('ServerError', () => {
    it('should create error with default status code', () => {
      const error = new ServerError('Internal server error');
      expect(error.message).toBe('Internal server error');
      expect(error.code).toBe('SERVER_ERROR');
      expect(error.statusCode).toBe(500);
    });

    it('should create error with custom status code', () => {
      const error = new ServerError('Service unavailable', 503);
      expect(error.statusCode).toBe(503);
    });
  });

  describe('TimeoutError', () => {
    it('should create error with timeout message', () => {
      const error = new TimeoutError('Request timeout');
      expect(error.message).toBe('Request timeout');
      expect(error.code).toBe('TIMEOUT_ERROR');
      expect(error.statusCode).toBe(408);
    });
  });
});
