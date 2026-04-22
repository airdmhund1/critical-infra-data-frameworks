package com.criticalinfra.config

/**
 * In-memory test double for [[SecretsResolver]] backed by a pre-configured map.
 *
 * Construct an instance with every `ref → value` pair that the test scenario requires.
 * Any `resolve` call whose `ref` is not present in the map returns a
 * [[SecretsResolutionError]], allowing tests to exercise both happy-path and
 * error-path branches without a live secrets manager.
 *
 * This class lives in the test source tree and must never be shipped in production JARs.
 *
 * Example usage:
 * {{{
 *   val resolver = new MockSecretsResolver(
 *     Map("vault://secret/data/oracle-prod" -> "s3cr3t")
 *   )
 *   resolver.resolve("vault://secret/data/oracle-prod")
 *   // => Right("s3cr3t")
 *
 *   resolver.resolve("vault://secret/data/does-not-exist")
 *   // => Left(SecretsResolutionError("vault://secret/data/does-not-exist",
 *   //         "No secret configured for ref: vault://secret/data/does-not-exist"))
 * }}}
 *
 * @param secrets Pre-configured map of secrets reference strings to plaintext values.
 */
final class MockSecretsResolver(secrets: Map[String, String]) extends SecretsResolver {

  /**
   * Returns the plaintext value for `ref` if it exists in the pre-configured map,
   * or a [[SecretsResolutionError]] if it does not.
   *
   * @param ref The secrets reference to look up.
   * @return    `Right(value)` if `ref` is present in the map, or
   *            `Left(SecretsResolutionError(ref, message))` if it is absent.
   */
  def resolve(ref: String): Either[SecretsResolutionError, String] =
    secrets.get(ref) match {
      case Some(value) => Right(value)
      case None =>
        Left(SecretsResolutionError(ref, s"No secret configured for ref: $ref"))
    }
}
