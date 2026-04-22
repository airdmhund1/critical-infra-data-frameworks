package com.criticalinfra.config

/**
 * Resolves a secrets reference string to its plaintext secret value at runtime.
 *
 * Implementations back this trait against concrete secrets managers — for example
 * HashiCorp Vault (using a `vault://` URI scheme) or AWS KMS (using an ARN). The
 * ingestion engine calls `resolve` immediately after loading a `SourceConfig`, using
 * the value of `Connection.credentialsRef` as the `ref` argument. If resolution
 * fails the engine must abort; a pipeline must never start with unresolved credentials.
 *
 * No credentials are accepted as constructor arguments or held as fields. All secret
 * material is obtained exclusively through the resolved `Right` value and must be
 * discarded when it is no longer needed.
 *
 * Implementations are expected to be safe for concurrent use by multiple Spark tasks.
 */
trait SecretsResolver {

  /**
   * Resolves a secrets reference to the plaintext secret value.
   *
   * @param ref An opaque reference understood by the backing secrets manager, such as
   *            a Vault path (`"vault://secret/data/oracle-tradedb-prod"`) or an AWS
   *            Secrets Manager ARN. The format is determined by the implementation.
   * @return    `Right(secret)` containing the resolved plaintext value on success, or
   *            `Left(SecretsResolutionError)` if the reference cannot be resolved — for
   *            example because the path does not exist, the caller lacks permission, or
   *            the secrets manager is unreachable.
   */
  def resolve(ref: String): Either[SecretsResolutionError, String]
}
