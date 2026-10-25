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
  `common.ErrCredentialInSettings` and `common.ErrInvalidArgument`. Server
  archive destination settings, the C library and replication policies can no
  longer carry credentials through their settings maps and use the SDK default
  chain.
- **BREAKING:** `cli.Config.StorageSettings` returns only `map[string]string`
  (no error) and contains no credential entries; `GetStorageSettings` is the
  same and stays deprecated. `cli.Config.Credentials` returns the handles to pass
  to `Configure` plus a release func. A destroyed enclave fails closed with
  `*cli.CredentialUnavailableError`, and a destroyed or empty handle given to a
  backend fails with `*common.CredentialUnavailableError`; the SDK default chain
  is never substituted for an unusable credential.
