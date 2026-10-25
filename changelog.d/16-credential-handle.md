### Changed

- **BREAKING:** `Storage.Configure` takes the credential as a protected-memory
  handle instead of a string, issue #16. The signature is now
  `Configure(settings map[string]string, creds common.Credentials) error`, where
  `common.Credentials{Key, Secret enclave.ByteAccess}` carries go-enclave
  handles. Plain settings (path, bucket, region, endpoint and so on) stay in the
  map; a credential never travels as a map value or any other `string`. S3 reads
  `Key` (access key) and `Secret` (secret key); Azure and Azure Archive read
  `Secret` (account key); a nil field uses the SDK's default credential chain.
  `factory.NewStorage`, `NewArchiver`, `NewBucketManager`, the `Creator` types,
  `s3.NewManager`, `azure.NewManager` and `objstore.BackendConfig` take the same
  `Credentials`, and `encryptedStorage` passes it through. A backend copies a
  credential out of its handle only for the call that builds the SDK credential,
  zeroizes the copy, and retains neither the handle nor the bytes. The vendor
  SDK's own copy is the accepted residual, recorded in
  `docs/architecture/protected-memory.md`. Every `Storage` implementation
  (mocks included) must change its `Configure` signature.
- **BREAKING:** a credential key in the settings map (`accessKey`, `accessKeyId`,
  `secretKey`, `secretAccessKey`, `accountKey`, `sasToken`, `sessionToken`, in
  any case, with or without `_` or `-`) is rejected by every backend's
  `Configure` with `*common.CredentialSettingError`, which matches
  `common.ErrCredentialInSettings` and `common.ErrInvalidArgument`.
- Server archive `DestinationSettings` (REST, gRPC, QUIC, unix, MCP) take the
  credential through the handle. `factory.NewArchiverFromWire` moves credential
  entries out of the request map into go-enclave handles at the API boundary,
  builds the archiver, and destroys the handles when the call ends; the wire
  format is unchanged. A token (`sessionToken`, `sasToken`) left in the
  settings is refused, and the error names the setting, never its value.
- C library: new export `ObjstoreNewStorageWithCredentials` takes the key and
  secret as length-prefixed buffers, copies them into a handle for the call and
  zeroizes its own copies. The caller wipes its own buffers. `ObjstoreNewStorage`
  and every existing symbol are unchanged, and it still refuses a credential in
  the settings arrays.
- Replication takes its credentials from a `replication.CredentialProvider`
  that the embedding application supplies when it constructs the manager
  (`replication.WithCredentialProvider`, an option to
  `NewPersistentReplicationManager`; `ReplicationConfig.CredentialProvider` on
  the facade). Each sync asks it for one handle per target, passes the handle to
  `Configure` and destroys it when the backend is configured. The provider is
  never persisted, a provider failure fails the sync with
  `*replication.CredentialProviderError` (no credential in its text), and a nil
  provider keeps the SDK default chain. `NewSyncerWithProvider` is the same
  path for a single syncer.
  `PersistentReplicationManager.AddPolicy` now refuses a credential in a
  policy's settings with `*common.CredentialSettingError`, so no secret is
  persisted.
- **BREAKING:** `cli.Config.StorageSettings` returns only `map[string]string`
  (no error) and contains no credential entries; `GetStorageSettings` is the
  same and stays deprecated. `cli.Config.Credentials` returns the handles to pass
  to `Configure` plus a release func. A destroyed enclave fails closed with
  `*cli.CredentialUnavailableError`, and a destroyed or empty handle given to a
  backend fails with `*common.CredentialUnavailableError`; the SDK default chain
  is never substituted for an unusable credential.
