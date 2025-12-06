// GENERATED CODE -- DO NOT EDIT!

'use strict';
var grpc = require('@grpc/grpc-js');
var objstore_pb = require('./objstore_pb.js');
var google_protobuf_timestamp_pb = require('google-protobuf/google/protobuf/timestamp_pb.js');

function serialize_objstore_v1_AddPolicyRequest(arg) {
  if (!(arg instanceof objstore_pb.AddPolicyRequest)) {
    throw new Error('Expected argument of type objstore.v1.AddPolicyRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_AddPolicyRequest(buffer_arg) {
  return objstore_pb.AddPolicyRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_AddPolicyResponse(arg) {
  if (!(arg instanceof objstore_pb.AddPolicyResponse)) {
    throw new Error('Expected argument of type objstore.v1.AddPolicyResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_AddPolicyResponse(buffer_arg) {
  return objstore_pb.AddPolicyResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_AddReplicationPolicyRequest(arg) {
  if (!(arg instanceof objstore_pb.AddReplicationPolicyRequest)) {
    throw new Error('Expected argument of type objstore.v1.AddReplicationPolicyRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_AddReplicationPolicyRequest(buffer_arg) {
  return objstore_pb.AddReplicationPolicyRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_AddReplicationPolicyResponse(arg) {
  if (!(arg instanceof objstore_pb.AddReplicationPolicyResponse)) {
    throw new Error('Expected argument of type objstore.v1.AddReplicationPolicyResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_AddReplicationPolicyResponse(buffer_arg) {
  return objstore_pb.AddReplicationPolicyResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_ApplyPoliciesRequest(arg) {
  if (!(arg instanceof objstore_pb.ApplyPoliciesRequest)) {
    throw new Error('Expected argument of type objstore.v1.ApplyPoliciesRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_ApplyPoliciesRequest(buffer_arg) {
  return objstore_pb.ApplyPoliciesRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_ApplyPoliciesResponse(arg) {
  if (!(arg instanceof objstore_pb.ApplyPoliciesResponse)) {
    throw new Error('Expected argument of type objstore.v1.ApplyPoliciesResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_ApplyPoliciesResponse(buffer_arg) {
  return objstore_pb.ApplyPoliciesResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_ArchiveRequest(arg) {
  if (!(arg instanceof objstore_pb.ArchiveRequest)) {
    throw new Error('Expected argument of type objstore.v1.ArchiveRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_ArchiveRequest(buffer_arg) {
  return objstore_pb.ArchiveRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_ArchiveResponse(arg) {
  if (!(arg instanceof objstore_pb.ArchiveResponse)) {
    throw new Error('Expected argument of type objstore.v1.ArchiveResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_ArchiveResponse(buffer_arg) {
  return objstore_pb.ArchiveResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_DeleteRequest(arg) {
  if (!(arg instanceof objstore_pb.DeleteRequest)) {
    throw new Error('Expected argument of type objstore.v1.DeleteRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_DeleteRequest(buffer_arg) {
  return objstore_pb.DeleteRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_DeleteResponse(arg) {
  if (!(arg instanceof objstore_pb.DeleteResponse)) {
    throw new Error('Expected argument of type objstore.v1.DeleteResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_DeleteResponse(buffer_arg) {
  return objstore_pb.DeleteResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_ExistsRequest(arg) {
  if (!(arg instanceof objstore_pb.ExistsRequest)) {
    throw new Error('Expected argument of type objstore.v1.ExistsRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_ExistsRequest(buffer_arg) {
  return objstore_pb.ExistsRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_ExistsResponse(arg) {
  if (!(arg instanceof objstore_pb.ExistsResponse)) {
    throw new Error('Expected argument of type objstore.v1.ExistsResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_ExistsResponse(buffer_arg) {
  return objstore_pb.ExistsResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_GetMetadataRequest(arg) {
  if (!(arg instanceof objstore_pb.GetMetadataRequest)) {
    throw new Error('Expected argument of type objstore.v1.GetMetadataRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_GetMetadataRequest(buffer_arg) {
  return objstore_pb.GetMetadataRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_GetPoliciesRequest(arg) {
  if (!(arg instanceof objstore_pb.GetPoliciesRequest)) {
    throw new Error('Expected argument of type objstore.v1.GetPoliciesRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_GetPoliciesRequest(buffer_arg) {
  return objstore_pb.GetPoliciesRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_GetPoliciesResponse(arg) {
  if (!(arg instanceof objstore_pb.GetPoliciesResponse)) {
    throw new Error('Expected argument of type objstore.v1.GetPoliciesResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_GetPoliciesResponse(buffer_arg) {
  return objstore_pb.GetPoliciesResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_GetReplicationPoliciesRequest(arg) {
  if (!(arg instanceof objstore_pb.GetReplicationPoliciesRequest)) {
    throw new Error('Expected argument of type objstore.v1.GetReplicationPoliciesRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_GetReplicationPoliciesRequest(buffer_arg) {
  return objstore_pb.GetReplicationPoliciesRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_GetReplicationPoliciesResponse(arg) {
  if (!(arg instanceof objstore_pb.GetReplicationPoliciesResponse)) {
    throw new Error('Expected argument of type objstore.v1.GetReplicationPoliciesResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_GetReplicationPoliciesResponse(buffer_arg) {
  return objstore_pb.GetReplicationPoliciesResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_GetReplicationPolicyRequest(arg) {
  if (!(arg instanceof objstore_pb.GetReplicationPolicyRequest)) {
    throw new Error('Expected argument of type objstore.v1.GetReplicationPolicyRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_GetReplicationPolicyRequest(buffer_arg) {
  return objstore_pb.GetReplicationPolicyRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_GetReplicationPolicyResponse(arg) {
  if (!(arg instanceof objstore_pb.GetReplicationPolicyResponse)) {
    throw new Error('Expected argument of type objstore.v1.GetReplicationPolicyResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_GetReplicationPolicyResponse(buffer_arg) {
  return objstore_pb.GetReplicationPolicyResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_GetReplicationStatusRequest(arg) {
  if (!(arg instanceof objstore_pb.GetReplicationStatusRequest)) {
    throw new Error('Expected argument of type objstore.v1.GetReplicationStatusRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_GetReplicationStatusRequest(buffer_arg) {
  return objstore_pb.GetReplicationStatusRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_GetReplicationStatusResponse(arg) {
  if (!(arg instanceof objstore_pb.GetReplicationStatusResponse)) {
    throw new Error('Expected argument of type objstore.v1.GetReplicationStatusResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_GetReplicationStatusResponse(buffer_arg) {
  return objstore_pb.GetReplicationStatusResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_GetRequest(arg) {
  if (!(arg instanceof objstore_pb.GetRequest)) {
    throw new Error('Expected argument of type objstore.v1.GetRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_GetRequest(buffer_arg) {
  return objstore_pb.GetRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_GetResponse(arg) {
  if (!(arg instanceof objstore_pb.GetResponse)) {
    throw new Error('Expected argument of type objstore.v1.GetResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_GetResponse(buffer_arg) {
  return objstore_pb.GetResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_HealthRequest(arg) {
  if (!(arg instanceof objstore_pb.HealthRequest)) {
    throw new Error('Expected argument of type objstore.v1.HealthRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_HealthRequest(buffer_arg) {
  return objstore_pb.HealthRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_HealthResponse(arg) {
  if (!(arg instanceof objstore_pb.HealthResponse)) {
    throw new Error('Expected argument of type objstore.v1.HealthResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_HealthResponse(buffer_arg) {
  return objstore_pb.HealthResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_ListRequest(arg) {
  if (!(arg instanceof objstore_pb.ListRequest)) {
    throw new Error('Expected argument of type objstore.v1.ListRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_ListRequest(buffer_arg) {
  return objstore_pb.ListRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_ListResponse(arg) {
  if (!(arg instanceof objstore_pb.ListResponse)) {
    throw new Error('Expected argument of type objstore.v1.ListResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_ListResponse(buffer_arg) {
  return objstore_pb.ListResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_MetadataResponse(arg) {
  if (!(arg instanceof objstore_pb.MetadataResponse)) {
    throw new Error('Expected argument of type objstore.v1.MetadataResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_MetadataResponse(buffer_arg) {
  return objstore_pb.MetadataResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_PutRequest(arg) {
  if (!(arg instanceof objstore_pb.PutRequest)) {
    throw new Error('Expected argument of type objstore.v1.PutRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_PutRequest(buffer_arg) {
  return objstore_pb.PutRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_PutResponse(arg) {
  if (!(arg instanceof objstore_pb.PutResponse)) {
    throw new Error('Expected argument of type objstore.v1.PutResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_PutResponse(buffer_arg) {
  return objstore_pb.PutResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_RemovePolicyRequest(arg) {
  if (!(arg instanceof objstore_pb.RemovePolicyRequest)) {
    throw new Error('Expected argument of type objstore.v1.RemovePolicyRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_RemovePolicyRequest(buffer_arg) {
  return objstore_pb.RemovePolicyRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_RemovePolicyResponse(arg) {
  if (!(arg instanceof objstore_pb.RemovePolicyResponse)) {
    throw new Error('Expected argument of type objstore.v1.RemovePolicyResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_RemovePolicyResponse(buffer_arg) {
  return objstore_pb.RemovePolicyResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_RemoveReplicationPolicyRequest(arg) {
  if (!(arg instanceof objstore_pb.RemoveReplicationPolicyRequest)) {
    throw new Error('Expected argument of type objstore.v1.RemoveReplicationPolicyRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_RemoveReplicationPolicyRequest(buffer_arg) {
  return objstore_pb.RemoveReplicationPolicyRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_RemoveReplicationPolicyResponse(arg) {
  if (!(arg instanceof objstore_pb.RemoveReplicationPolicyResponse)) {
    throw new Error('Expected argument of type objstore.v1.RemoveReplicationPolicyResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_RemoveReplicationPolicyResponse(buffer_arg) {
  return objstore_pb.RemoveReplicationPolicyResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_TriggerReplicationRequest(arg) {
  if (!(arg instanceof objstore_pb.TriggerReplicationRequest)) {
    throw new Error('Expected argument of type objstore.v1.TriggerReplicationRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_TriggerReplicationRequest(buffer_arg) {
  return objstore_pb.TriggerReplicationRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_TriggerReplicationResponse(arg) {
  if (!(arg instanceof objstore_pb.TriggerReplicationResponse)) {
    throw new Error('Expected argument of type objstore.v1.TriggerReplicationResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_TriggerReplicationResponse(buffer_arg) {
  return objstore_pb.TriggerReplicationResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_UpdateMetadataRequest(arg) {
  if (!(arg instanceof objstore_pb.UpdateMetadataRequest)) {
    throw new Error('Expected argument of type objstore.v1.UpdateMetadataRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_UpdateMetadataRequest(buffer_arg) {
  return objstore_pb.UpdateMetadataRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_objstore_v1_UpdateMetadataResponse(arg) {
  if (!(arg instanceof objstore_pb.UpdateMetadataResponse)) {
    throw new Error('Expected argument of type objstore.v1.UpdateMetadataResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_objstore_v1_UpdateMetadataResponse(buffer_arg) {
  return objstore_pb.UpdateMetadataResponse.deserializeBinary(new Uint8Array(buffer_arg));
}


// ObjectStore service provides a unified interface for object storage operations.
var ObjectStoreService = exports.ObjectStoreService = {
  // Put stores an object in the backend.
put: {
    path: '/objstore.v1.ObjectStore/Put',
    requestStream: false,
    responseStream: false,
    requestType: objstore_pb.PutRequest,
    responseType: objstore_pb.PutResponse,
    requestSerialize: serialize_objstore_v1_PutRequest,
    requestDeserialize: deserialize_objstore_v1_PutRequest,
    responseSerialize: serialize_objstore_v1_PutResponse,
    responseDeserialize: deserialize_objstore_v1_PutResponse,
  },
  // Get retrieves an object from the backend with streaming support for large files.
get: {
    path: '/objstore.v1.ObjectStore/Get',
    requestStream: false,
    responseStream: true,
    requestType: objstore_pb.GetRequest,
    responseType: objstore_pb.GetResponse,
    requestSerialize: serialize_objstore_v1_GetRequest,
    requestDeserialize: deserialize_objstore_v1_GetRequest,
    responseSerialize: serialize_objstore_v1_GetResponse,
    responseDeserialize: deserialize_objstore_v1_GetResponse,
  },
  // Delete removes an object from the backend.
delete: {
    path: '/objstore.v1.ObjectStore/Delete',
    requestStream: false,
    responseStream: false,
    requestType: objstore_pb.DeleteRequest,
    responseType: objstore_pb.DeleteResponse,
    requestSerialize: serialize_objstore_v1_DeleteRequest,
    requestDeserialize: deserialize_objstore_v1_DeleteRequest,
    responseSerialize: serialize_objstore_v1_DeleteResponse,
    responseDeserialize: deserialize_objstore_v1_DeleteResponse,
  },
  // List returns a list of objects that match the given criteria.
list: {
    path: '/objstore.v1.ObjectStore/List',
    requestStream: false,
    responseStream: false,
    requestType: objstore_pb.ListRequest,
    responseType: objstore_pb.ListResponse,
    requestSerialize: serialize_objstore_v1_ListRequest,
    requestDeserialize: deserialize_objstore_v1_ListRequest,
    responseSerialize: serialize_objstore_v1_ListResponse,
    responseDeserialize: deserialize_objstore_v1_ListResponse,
  },
  // Exists checks if an object exists in the backend.
exists: {
    path: '/objstore.v1.ObjectStore/Exists',
    requestStream: false,
    responseStream: false,
    requestType: objstore_pb.ExistsRequest,
    responseType: objstore_pb.ExistsResponse,
    requestSerialize: serialize_objstore_v1_ExistsRequest,
    requestDeserialize: deserialize_objstore_v1_ExistsRequest,
    responseSerialize: serialize_objstore_v1_ExistsResponse,
    responseDeserialize: deserialize_objstore_v1_ExistsResponse,
  },
  // GetMetadata retrieves only the metadata for an object without its content.
getMetadata: {
    path: '/objstore.v1.ObjectStore/GetMetadata',
    requestStream: false,
    responseStream: false,
    requestType: objstore_pb.GetMetadataRequest,
    responseType: objstore_pb.MetadataResponse,
    requestSerialize: serialize_objstore_v1_GetMetadataRequest,
    requestDeserialize: deserialize_objstore_v1_GetMetadataRequest,
    responseSerialize: serialize_objstore_v1_MetadataResponse,
    responseDeserialize: deserialize_objstore_v1_MetadataResponse,
  },
  // UpdateMetadata updates the metadata for an existing object.
updateMetadata: {
    path: '/objstore.v1.ObjectStore/UpdateMetadata',
    requestStream: false,
    responseStream: false,
    requestType: objstore_pb.UpdateMetadataRequest,
    responseType: objstore_pb.UpdateMetadataResponse,
    requestSerialize: serialize_objstore_v1_UpdateMetadataRequest,
    requestDeserialize: deserialize_objstore_v1_UpdateMetadataRequest,
    responseSerialize: serialize_objstore_v1_UpdateMetadataResponse,
    responseDeserialize: deserialize_objstore_v1_UpdateMetadataResponse,
  },
  // Health check endpoint for service health monitoring.
health: {
    path: '/objstore.v1.ObjectStore/Health',
    requestStream: false,
    responseStream: false,
    requestType: objstore_pb.HealthRequest,
    responseType: objstore_pb.HealthResponse,
    requestSerialize: serialize_objstore_v1_HealthRequest,
    requestDeserialize: deserialize_objstore_v1_HealthRequest,
    responseSerialize: serialize_objstore_v1_HealthResponse,
    responseDeserialize: deserialize_objstore_v1_HealthResponse,
  },
  // Archive copies an object to an archival storage backend.
archive: {
    path: '/objstore.v1.ObjectStore/Archive',
    requestStream: false,
    responseStream: false,
    requestType: objstore_pb.ArchiveRequest,
    responseType: objstore_pb.ArchiveResponse,
    requestSerialize: serialize_objstore_v1_ArchiveRequest,
    requestDeserialize: deserialize_objstore_v1_ArchiveRequest,
    responseSerialize: serialize_objstore_v1_ArchiveResponse,
    responseDeserialize: deserialize_objstore_v1_ArchiveResponse,
  },
  // AddPolicy adds a new lifecycle policy.
addPolicy: {
    path: '/objstore.v1.ObjectStore/AddPolicy',
    requestStream: false,
    responseStream: false,
    requestType: objstore_pb.AddPolicyRequest,
    responseType: objstore_pb.AddPolicyResponse,
    requestSerialize: serialize_objstore_v1_AddPolicyRequest,
    requestDeserialize: deserialize_objstore_v1_AddPolicyRequest,
    responseSerialize: serialize_objstore_v1_AddPolicyResponse,
    responseDeserialize: deserialize_objstore_v1_AddPolicyResponse,
  },
  // RemovePolicy removes an existing lifecycle policy.
removePolicy: {
    path: '/objstore.v1.ObjectStore/RemovePolicy',
    requestStream: false,
    responseStream: false,
    requestType: objstore_pb.RemovePolicyRequest,
    responseType: objstore_pb.RemovePolicyResponse,
    requestSerialize: serialize_objstore_v1_RemovePolicyRequest,
    requestDeserialize: deserialize_objstore_v1_RemovePolicyRequest,
    responseSerialize: serialize_objstore_v1_RemovePolicyResponse,
    responseDeserialize: deserialize_objstore_v1_RemovePolicyResponse,
  },
  // GetPolicies retrieves all lifecycle policies.
getPolicies: {
    path: '/objstore.v1.ObjectStore/GetPolicies',
    requestStream: false,
    responseStream: false,
    requestType: objstore_pb.GetPoliciesRequest,
    responseType: objstore_pb.GetPoliciesResponse,
    requestSerialize: serialize_objstore_v1_GetPoliciesRequest,
    requestDeserialize: deserialize_objstore_v1_GetPoliciesRequest,
    responseSerialize: serialize_objstore_v1_GetPoliciesResponse,
    responseDeserialize: deserialize_objstore_v1_GetPoliciesResponse,
  },
  // ApplyPolicies executes all lifecycle policies.
applyPolicies: {
    path: '/objstore.v1.ObjectStore/ApplyPolicies',
    requestStream: false,
    responseStream: false,
    requestType: objstore_pb.ApplyPoliciesRequest,
    responseType: objstore_pb.ApplyPoliciesResponse,
    requestSerialize: serialize_objstore_v1_ApplyPoliciesRequest,
    requestDeserialize: deserialize_objstore_v1_ApplyPoliciesRequest,
    responseSerialize: serialize_objstore_v1_ApplyPoliciesResponse,
    responseDeserialize: deserialize_objstore_v1_ApplyPoliciesResponse,
  },
  // Replication methods
//
// AddReplicationPolicy adds a new replication policy.
addReplicationPolicy: {
    path: '/objstore.v1.ObjectStore/AddReplicationPolicy',
    requestStream: false,
    responseStream: false,
    requestType: objstore_pb.AddReplicationPolicyRequest,
    responseType: objstore_pb.AddReplicationPolicyResponse,
    requestSerialize: serialize_objstore_v1_AddReplicationPolicyRequest,
    requestDeserialize: deserialize_objstore_v1_AddReplicationPolicyRequest,
    responseSerialize: serialize_objstore_v1_AddReplicationPolicyResponse,
    responseDeserialize: deserialize_objstore_v1_AddReplicationPolicyResponse,
  },
  // RemoveReplicationPolicy removes an existing replication policy.
removeReplicationPolicy: {
    path: '/objstore.v1.ObjectStore/RemoveReplicationPolicy',
    requestStream: false,
    responseStream: false,
    requestType: objstore_pb.RemoveReplicationPolicyRequest,
    responseType: objstore_pb.RemoveReplicationPolicyResponse,
    requestSerialize: serialize_objstore_v1_RemoveReplicationPolicyRequest,
    requestDeserialize: deserialize_objstore_v1_RemoveReplicationPolicyRequest,
    responseSerialize: serialize_objstore_v1_RemoveReplicationPolicyResponse,
    responseDeserialize: deserialize_objstore_v1_RemoveReplicationPolicyResponse,
  },
  // GetReplicationPolicies retrieves all replication policies.
getReplicationPolicies: {
    path: '/objstore.v1.ObjectStore/GetReplicationPolicies',
    requestStream: false,
    responseStream: false,
    requestType: objstore_pb.GetReplicationPoliciesRequest,
    responseType: objstore_pb.GetReplicationPoliciesResponse,
    requestSerialize: serialize_objstore_v1_GetReplicationPoliciesRequest,
    requestDeserialize: deserialize_objstore_v1_GetReplicationPoliciesRequest,
    responseSerialize: serialize_objstore_v1_GetReplicationPoliciesResponse,
    responseDeserialize: deserialize_objstore_v1_GetReplicationPoliciesResponse,
  },
  // GetReplicationPolicy retrieves a specific replication policy.
getReplicationPolicy: {
    path: '/objstore.v1.ObjectStore/GetReplicationPolicy',
    requestStream: false,
    responseStream: false,
    requestType: objstore_pb.GetReplicationPolicyRequest,
    responseType: objstore_pb.GetReplicationPolicyResponse,
    requestSerialize: serialize_objstore_v1_GetReplicationPolicyRequest,
    requestDeserialize: deserialize_objstore_v1_GetReplicationPolicyRequest,
    responseSerialize: serialize_objstore_v1_GetReplicationPolicyResponse,
    responseDeserialize: deserialize_objstore_v1_GetReplicationPolicyResponse,
  },
  // TriggerReplication triggers synchronization for one or all policies.
triggerReplication: {
    path: '/objstore.v1.ObjectStore/TriggerReplication',
    requestStream: false,
    responseStream: false,
    requestType: objstore_pb.TriggerReplicationRequest,
    responseType: objstore_pb.TriggerReplicationResponse,
    requestSerialize: serialize_objstore_v1_TriggerReplicationRequest,
    requestDeserialize: deserialize_objstore_v1_TriggerReplicationRequest,
    responseSerialize: serialize_objstore_v1_TriggerReplicationResponse,
    responseDeserialize: deserialize_objstore_v1_TriggerReplicationResponse,
  },
  // GetReplicationStatus retrieves status and metrics for a specific replication policy.
getReplicationStatus: {
    path: '/objstore.v1.ObjectStore/GetReplicationStatus',
    requestStream: false,
    responseStream: false,
    requestType: objstore_pb.GetReplicationStatusRequest,
    responseType: objstore_pb.GetReplicationStatusResponse,
    requestSerialize: serialize_objstore_v1_GetReplicationStatusRequest,
    requestDeserialize: deserialize_objstore_v1_GetReplicationStatusRequest,
    responseSerialize: serialize_objstore_v1_GetReplicationStatusResponse,
    responseDeserialize: deserialize_objstore_v1_GetReplicationStatusResponse,
  },
};

exports.ObjectStoreClient = grpc.makeGenericClientConstructor(ObjectStoreService, 'ObjectStore');
