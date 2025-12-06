import {
  validateKey,
  validateData,
  validatePositiveNumber,
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
} from '../../src/validation';
import { ReplicationMode } from '../../src/types';

describe('Validation Functions', () => {
  describe('validateKey', () => {
    it('should pass for valid key', () => {
      expect(() => validateKey('valid-key')).not.toThrow();
    });

    it('should throw for non-string key', () => {
      expect(() => validateKey(123 as any)).toThrow('key must be a string');
    });

    it('should throw for empty key', () => {
      expect(() => validateKey('')).toThrow('key must not be empty');
    });

    it('should throw for whitespace-only key', () => {
      expect(() => validateKey('   ')).toThrow('key must not be empty');
    });

    it('should use custom field name in error', () => {
      expect(() => validateKey('', 'objectKey')).toThrow('objectKey must not be empty');
    });
  });

  describe('validateData', () => {
    it('should pass for Buffer', () => {
      expect(() => validateData(Buffer.from('test'))).not.toThrow();
    });

    it('should pass for Uint8Array', () => {
      expect(() => validateData(new Uint8Array([1, 2, 3]))).not.toThrow();
    });

    it('should throw for non-Buffer/Uint8Array', () => {
      expect(() => validateData('string' as any)).toThrow(
        'data must be a Buffer or Uint8Array'
      );
    });

    it('should throw for number', () => {
      expect(() => validateData(123 as any)).toThrow();
    });

    it('should throw for object', () => {
      expect(() => validateData({} as any)).toThrow();
    });
  });

  describe('validatePositiveNumber', () => {
    it('should pass for positive number', () => {
      expect(() => validatePositiveNumber(10, 'count')).not.toThrow();
    });

    it('should pass for zero', () => {
      expect(() => validatePositiveNumber(0, 'count')).not.toThrow();
    });

    it('should throw for negative number', () => {
      expect(() => validatePositiveNumber(-1, 'count')).toThrow(
        'count must be a positive number'
      );
    });

    it('should throw for non-number', () => {
      expect(() => validatePositiveNumber('10' as any, 'count')).toThrow(
        'count must be a finite number'
      );
    });

    it('should throw for Infinity', () => {
      expect(() => validatePositiveNumber(Infinity, 'count')).toThrow(
        'count must be a finite number'
      );
    });

    it('should throw for NaN', () => {
      expect(() => validatePositiveNumber(NaN, 'count')).toThrow(
        'count must be a finite number'
      );
    });
  });

  describe('validatePutRequest', () => {
    it('should pass for valid request', () => {
      expect(() =>
        validatePutRequest({ key: 'test', data: Buffer.from('data') })
      ).not.toThrow();
    });

    it('should throw for null request', () => {
      expect(() => validatePutRequest(null as any)).toThrow('request must be an object');
    });

    it('should throw for invalid key', () => {
      expect(() => validatePutRequest({ key: '', data: Buffer.from('data') })).toThrow();
    });

    it('should throw for invalid data', () => {
      expect(() => validatePutRequest({ key: 'test', data: 'string' as any })).toThrow();
    });
  });

  describe('validateGetRequest', () => {
    it('should pass for valid request', () => {
      expect(() => validateGetRequest({ key: 'test' })).not.toThrow();
    });

    it('should throw for null request', () => {
      expect(() => validateGetRequest(null as any)).toThrow('request must be an object');
    });

    it('should throw for invalid key', () => {
      expect(() => validateGetRequest({ key: '' })).toThrow();
    });
  });

  describe('validateDeleteRequest', () => {
    it('should pass for valid request', () => {
      expect(() => validateDeleteRequest({ key: 'test' })).not.toThrow();
    });

    it('should throw for null request', () => {
      expect(() => validateDeleteRequest(null as any)).toThrow('request must be an object');
    });

    it('should throw for invalid key', () => {
      expect(() => validateDeleteRequest({ key: '' })).toThrow();
    });
  });

  describe('validateListRequest', () => {
    it('should pass for empty request', () => {
      expect(() => validateListRequest({})).not.toThrow();
    });

    it('should pass for valid request with all fields', () => {
      expect(() =>
        validateListRequest({
          prefix: 'test/',
          delimiter: '/',
          maxResults: 10,
          continueFrom: 'token',
        })
      ).not.toThrow();
    });

    it('should throw for null request', () => {
      expect(() => validateListRequest(null as any)).toThrow('request must be an object');
    });

    it('should throw for non-string prefix', () => {
      expect(() => validateListRequest({ prefix: 123 as any })).toThrow(
        'prefix must be a string'
      );
    });

    it('should throw for non-string delimiter', () => {
      expect(() => validateListRequest({ delimiter: 123 as any })).toThrow(
        'delimiter must be a string'
      );
    });

    it('should throw for non-number maxResults', () => {
      expect(() => validateListRequest({ maxResults: '10' as any })).toThrow();
    });

    it('should throw for zero maxResults', () => {
      expect(() => validateListRequest({ maxResults: 0 })).toThrow(
        'maxResults must be greater than 0'
      );
    });

    it('should throw for non-string continueFrom', () => {
      expect(() => validateListRequest({ continueFrom: 123 as any })).toThrow(
        'continueFrom must be a string'
      );
    });
  });

  describe('validateExistsRequest', () => {
    it('should pass for valid request', () => {
      expect(() => validateExistsRequest({ key: 'test' })).not.toThrow();
    });

    it('should throw for null request', () => {
      expect(() => validateExistsRequest(null as any)).toThrow('request must be an object');
    });
  });

  describe('validateGetMetadataRequest', () => {
    it('should pass for valid request', () => {
      expect(() => validateGetMetadataRequest({ key: 'test' })).not.toThrow();
    });

    it('should throw for null request', () => {
      expect(() => validateGetMetadataRequest(null as any)).toThrow(
        'request must be an object'
      );
    });
  });

  describe('validateUpdateMetadataRequest', () => {
    it('should pass for valid request', () => {
      expect(() =>
        validateUpdateMetadataRequest({ key: 'test', metadata: { contentType: 'text/plain' } })
      ).not.toThrow();
    });

    it('should throw for null request', () => {
      expect(() => validateUpdateMetadataRequest(null as any)).toThrow(
        'request must be an object'
      );
    });

    it('should throw for missing metadata', () => {
      expect(() => validateUpdateMetadataRequest({ key: 'test', metadata: null as any })).toThrow(
        'metadata must be an object'
      );
    });

    it('should throw for invalid metadata type', () => {
      expect(() =>
        validateUpdateMetadataRequest({ key: 'test', metadata: 'string' as any })
      ).toThrow('metadata must be an object');
    });
  });

  describe('validateArchiveRequest', () => {
    it('should pass for valid request', () => {
      expect(() =>
        validateArchiveRequest({ key: 'test', destinationType: 'glacier' })
      ).not.toThrow();
    });

    it('should throw for null request', () => {
      expect(() => validateArchiveRequest(null as any)).toThrow('request must be an object');
    });

    it('should throw for empty destinationType', () => {
      expect(() => validateArchiveRequest({ key: 'test', destinationType: '' })).toThrow(
        'destinationType must be a non-empty string'
      );
    });

    it('should throw for whitespace-only destinationType', () => {
      expect(() => validateArchiveRequest({ key: 'test', destinationType: '   ' })).toThrow(
        'destinationType must be a non-empty string'
      );
    });
  });

  describe('validateAddPolicyRequest', () => {
    it('should pass for valid request', () => {
      expect(() =>
        validateAddPolicyRequest({
          policy: {
            id: 'p1',
            prefix: 'logs/',
            retentionSeconds: 86400,
            action: 'delete',
          },
        })
      ).not.toThrow();
    });

    it('should throw for null request', () => {
      expect(() => validateAddPolicyRequest(null as any)).toThrow('request must be an object');
    });

    it('should throw for missing policy', () => {
      expect(() => validateAddPolicyRequest({ policy: null as any })).toThrow(
        'policy must be an object'
      );
    });

    it('should throw for empty policy id', () => {
      expect(() =>
        validateAddPolicyRequest({
          policy: { id: '', prefix: 'logs/', retentionSeconds: 86400, action: 'delete' },
        })
      ).toThrow();
    });

    it('should throw for non-string prefix', () => {
      expect(() =>
        validateAddPolicyRequest({
          policy: { id: 'p1', prefix: 123 as any, retentionSeconds: 86400, action: 'delete' },
        })
      ).toThrow('policy.prefix must be a string');
    });

    it('should throw for negative retentionSeconds', () => {
      expect(() =>
        validateAddPolicyRequest({
          policy: { id: 'p1', prefix: 'logs/', retentionSeconds: -1, action: 'delete' },
        })
      ).toThrow();
    });

    it('should throw for empty action', () => {
      expect(() =>
        validateAddPolicyRequest({
          policy: { id: 'p1', prefix: 'logs/', retentionSeconds: 86400, action: '' },
        })
      ).toThrow('policy.action must be a non-empty string');
    });
  });

  describe('validateRemovePolicyRequest', () => {
    it('should pass for valid request', () => {
      expect(() => validateRemovePolicyRequest({ id: 'p1' })).not.toThrow();
    });

    it('should throw for null request', () => {
      expect(() => validateRemovePolicyRequest(null as any)).toThrow(
        'request must be an object'
      );
    });

    it('should throw for empty id', () => {
      expect(() => validateRemovePolicyRequest({ id: '' })).toThrow();
    });
  });

  describe('validateAddReplicationPolicyRequest', () => {
    it('should pass for valid request', () => {
      expect(() =>
        validateAddReplicationPolicyRequest({
          policy: {
            id: 'r1',
            sourceBackend: 's3',
            sourceSettings: { bucket: 'source' },
            sourcePrefix: '',
            destinationBackend: 'gcs',
            destinationSettings: { bucket: 'dest' },
            checkIntervalSeconds: 3600,
            enabled: true,
            replicationMode: ReplicationMode.TRANSPARENT,
          },
        })
      ).not.toThrow();
    });

    it('should throw for null request', () => {
      expect(() => validateAddReplicationPolicyRequest(null as any)).toThrow(
        'request must be an object'
      );
    });

    it('should throw for missing policy', () => {
      expect(() => validateAddReplicationPolicyRequest({ policy: null as any })).toThrow(
        'policy must be an object'
      );
    });

    it('should throw for empty sourceBackend', () => {
      expect(() =>
        validateAddReplicationPolicyRequest({
          policy: {
            id: 'r1',
            sourceBackend: '',
            sourceSettings: {},
            sourcePrefix: '',
            destinationBackend: 'gcs',
            destinationSettings: {},
            checkIntervalSeconds: 3600,
            enabled: true,
            replicationMode: 0,
          },
        })
      ).toThrow('policy.sourceBackend must be a non-empty string');
    });

    it('should throw for empty destinationBackend', () => {
      expect(() =>
        validateAddReplicationPolicyRequest({
          policy: {
            id: 'r1',
            sourceBackend: 's3',
            sourceSettings: {},
            sourcePrefix: '',
            destinationBackend: '',
            destinationSettings: {},
            checkIntervalSeconds: 3600,
            enabled: true,
            replicationMode: 0,
          },
        })
      ).toThrow('policy.destinationBackend must be a non-empty string');
    });

    it('should throw for non-object sourceSettings', () => {
      expect(() =>
        validateAddReplicationPolicyRequest({
          policy: {
            id: 'r1',
            sourceBackend: 's3',
            sourceSettings: null as any,
            sourcePrefix: '',
            destinationBackend: 'gcs',
            destinationSettings: {},
            checkIntervalSeconds: 3600,
            enabled: true,
            replicationMode: 0,
          },
        })
      ).toThrow('policy.sourceSettings must be an object');
    });

    it('should throw for non-object destinationSettings', () => {
      expect(() =>
        validateAddReplicationPolicyRequest({
          policy: {
            id: 'r1',
            sourceBackend: 's3',
            sourceSettings: {},
            sourcePrefix: '',
            destinationBackend: 'gcs',
            destinationSettings: null as any,
            checkIntervalSeconds: 3600,
            enabled: true,
            replicationMode: 0,
          },
        })
      ).toThrow('policy.destinationSettings must be an object');
    });

    it('should throw for non-boolean enabled', () => {
      expect(() =>
        validateAddReplicationPolicyRequest({
          policy: {
            id: 'r1',
            sourceBackend: 's3',
            sourceSettings: {},
            sourcePrefix: '',
            destinationBackend: 'gcs',
            destinationSettings: {},
            checkIntervalSeconds: 3600,
            enabled: 'true' as any,
            replicationMode: 0,
          },
        })
      ).toThrow('policy.enabled must be a boolean');
    });
  });

  describe('validateRemoveReplicationPolicyRequest', () => {
    it('should pass for valid request', () => {
      expect(() => validateRemoveReplicationPolicyRequest({ id: 'r1' })).not.toThrow();
    });

    it('should throw for null request', () => {
      expect(() => validateRemoveReplicationPolicyRequest(null as any)).toThrow(
        'request must be an object'
      );
    });
  });

  describe('validateGetReplicationPolicyRequest', () => {
    it('should pass for valid request', () => {
      expect(() => validateGetReplicationPolicyRequest({ id: 'r1' })).not.toThrow();
    });

    it('should throw for null request', () => {
      expect(() => validateGetReplicationPolicyRequest(null as any)).toThrow(
        'request must be an object'
      );
    });
  });

  describe('validateGetReplicationStatusRequest', () => {
    it('should pass for valid request', () => {
      expect(() => validateGetReplicationStatusRequest({ id: 'r1' })).not.toThrow();
    });

    it('should throw for null request', () => {
      expect(() => validateGetReplicationStatusRequest(null as any)).toThrow(
        'request must be an object'
      );
    });
  });
});
