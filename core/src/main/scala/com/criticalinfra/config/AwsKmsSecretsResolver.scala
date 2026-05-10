package com.criticalinfra.config

import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.util.Try

// =============================================================================
// AwsKmsSecretsResolver — AWS Secrets Manager implementation of SecretsResolver
//
// Resolves `aws-secrets://`-scheme references against AWS Secrets Manager using
// AWS SDK v2 with DefaultCredentialsProvider (instance-profile, ECS task role,
// environment variables, and system properties — in that SDK-defined order).
// Supports exponential-backoff retry with an injectable delay function and
// strict log-safety guarantees: resolved secret values and credential material
// are never written to any log output.
// =============================================================================

// -----------------------------------------------------------------------------
// AwsSecretsClient — injectable abstraction for testability
// -----------------------------------------------------------------------------

/** Minimal abstraction over the AWS Secrets Manager API, designed for injection in tests.
  *
  * Implementations return `Right(secretString)` on success and `Left(errorMessage)` on any failure.
  * The returned `Right` value contains the raw secret string and must never be logged by the
  * caller.
  */
trait AwsSecretsClient {

  /** Retrieves the plaintext secret value for the given secret identifier.
    *
    * @param secretId
    *   The AWS Secrets Manager secret name or full ARN (without the `aws-secrets://` prefix).
    * @return
    *   `Right(secretString)` on success, `Left(errorMessage)` on any failure.
    */
  def getSecretValue(secretId: String): Either[String, String]
}

/** Production implementation of [[AwsSecretsClient]] backed by the AWS SDK v2
  * `SecretsManagerClient`.
  *
  * Credentials are resolved exclusively via `DefaultCredentialsProvider`, which checks, in order:
  *   1. Environment variables (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`) 2. Java system
  *      properties (`aws.accessKeyId` / `aws.secretAccessKey`) 3. ECS container credentials (task
  *      role) 4. EC2 instance metadata (instance profile / IMDSv2)
  *
  * No credentials are ever accepted as constructor arguments. This class is intentionally free of
  * any credential parameters so that it is safe to instantiate from configuration without risk of
  * credential leakage.
  *
  * The `UrlConnectionHttpClient` is used in preference to the default Netty async client, avoiding
  * pulling the full Netty stack (10MB+) onto the classpath.
  *
  * @param region
  *   AWS region override. When `None`, the SDK resolves the region from the environment (e.g.
  *   `AWS_DEFAULT_REGION` or EC2 instance metadata).
  */
final class DefaultAwsSecretsClient(region: Option[String] = None) extends AwsSecretsClient {

  import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
  import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
  import software.amazon.awssdk.regions.Region
  import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient
  import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest

  private val client: SecretsManagerClient = {
    val builder = SecretsManagerClient
      .builder()
      .credentialsProvider(DefaultCredentialsProvider.create())
      .httpClientBuilder(UrlConnectionHttpClient.builder())
    val configured = region.fold(builder)(r => builder.region(Region.of(r)))
    configured.build()
  }

  /** Retrieves the plaintext secret string from AWS Secrets Manager.
    *
    * Wraps the SDK call in `Try` so that any AWS SDK exception (e.g. `ResourceNotFoundException`,
    * `AccessDeniedException`, `SdkClientException`) is converted to `Left(message)` without
    * propagating an exception to the caller.
    *
    * @param secretId
    *   AWS secret name or ARN.
    * @return
    *   `Right(secretString)` on success, `Left(exceptionMessage)` on any failure.
    */
  def getSecretValue(secretId: String): Either[String, String] = {
    val request = GetSecretValueRequest
      .builder()
      .secretId(secretId)
      .build()
    Try(client.getSecretValue(request).secretString()).toEither.left.map(_.getMessage)
  }
}

/** Companion object for [[AwsSecretsClient]] — factory methods for common configurations.
  *
  * Use [[default]] when the AWS region can be resolved from the environment. Use [[withRegion]] to
  * explicitly specify the region (useful in multi-region deployments).
  */
object AwsSecretsClient {

  /** Returns an [[AwsSecretsClient]] that resolves the AWS region from the environment. */
  def default: AwsSecretsClient = new DefaultAwsSecretsClient()

  /** Returns an [[AwsSecretsClient]] configured for the specified AWS region.
    *
    * @param region
    *   AWS region identifier (e.g. `"us-east-1"`).
    */
  def withRegion(region: String): AwsSecretsClient = new DefaultAwsSecretsClient(Some(region))
}

// -----------------------------------------------------------------------------
// AwsKmsSecretsResolver — main implementation
// -----------------------------------------------------------------------------

/** Production implementation of [[SecretsResolver]] that resolves `aws-secrets://`-scheme
  * references against AWS Secrets Manager using AWS SDK v2.
  *
  * ==Usage==
  * {{{
  *   val resolver = new AwsKmsSecretsResolver(
  *     awsClient = AwsSecretsClient.withRegion("us-east-1")
  *   )
  *   resolver.resolve("aws-secrets://prod/oracle-tradedb/jdbc-credentials")
  *   // => Right("the-actual-password")
  * }}}
  *
  * ==Credential resolution==
  * Credentials are resolved via `DefaultCredentialsProvider` in the following order (no credentials
  * are ever accepted as constructor arguments):
  *   1. Environment variables (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`) 2. Java system
  *      properties (`aws.accessKeyId` / `aws.secretAccessKey`) 3. ECS container credentials (task
  *      role) 4. EC2 instance metadata (instance profile / IMDSv2)
  *
  * ==Retry behaviour==
  * Transient failures are retried up to `maxRetries` times with exponential back-off starting at
  * 500 ms. The delay function is injectable so that unit tests can set it to a no-op.
  *
  * ==Log safety==
  * The resolved secret value and any AWS credential material are '''never''' written to any log
  * output at any level. Only the `ref` path (the `aws-secrets://`-prefixed identifier) and
  * sanitised, fixed-template error messages are logged. Raw AWS SDK exception messages are never
  * propagated to log output because they may contain fragments of secret data.
  *
  * ==Thread safety==
  * Instances are safe for concurrent use from multiple Spark tasks.
  *
  * @param awsClient
  *   AWS Secrets Manager client. Defaults to [[AwsSecretsClient.default]]. Override in tests to
  *   inject a stub.
  * @param maxRetries
  *   Maximum number of retry attempts after the initial failure. Default: 3.
  * @param retryDelayFn
  *   Function called with the delay in milliseconds before each retry. Defaults to `Thread.sleep`.
  *   Override in tests with `_ => ()` for instant retries.
  */
final class AwsKmsSecretsResolver(
    awsClient: AwsSecretsClient = AwsSecretsClient.default,
    maxRetries: Int = 3,
    retryDelayFn: Long => Unit = Thread.sleep
) extends SecretsResolver {

  private val logger = LoggerFactory.getLogger(classOf[AwsKmsSecretsResolver])

  private val AwsScheme = "aws-secrets://"

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  /** Resolves an `aws-secrets://`-scheme reference to its plaintext secret value.
    *
    * Steps:
    *   1. Validates that `ref` starts with `"aws-secrets://"`. Returns `Left` immediately if not.
    *      2. Strips the scheme prefix to obtain the AWS secret name or ARN. 3. Calls AWS Secrets
    *      Manager via `awsClient.getSecretValue`, with exponential-backoff retry. 4. Returns
    *      `Right(secretString)` on success.
    *
    * The resolved secret value is never logged at any step.
    *
    * @param ref
    *   An `aws-secrets://`-prefixed reference, e.g.
    *   `"aws-secrets://prod/oracle-tradedb/jdbc-credentials"` or
    *   `"aws-secrets://arn:aws:secretsmanager:us-east-1:123456789012:secret:MySecret"`.
    * @return
    *   `Right(secret)` on success, `Left(SecretsResolutionError)` on any failure.
    */
  def resolve(ref: String): Either[SecretsResolutionError, String] = {
    if (!ref.startsWith(AwsScheme)) {
      logger.warn("Secrets ref does not use aws-secrets:// scheme: {}", ref)
      return Left(SecretsResolutionError(ref, s"ref does not use aws-secrets:// scheme"))
    }

    val secretId = ref.stripPrefix(AwsScheme)

    withRetry(ref, awsClient.getSecretValue(secretId), maxRetries, 500L) match {
      case Right(secretString) =>
        logger.debug("Successfully resolved secret for ref {}", ref)
        Right(secretString)
      case Left(cause) =>
        Left(SecretsResolutionError(ref, "AWS Secrets Manager call failed"))
    }
  }

  // ---------------------------------------------------------------------------
  // Private: retry with exponential back-off
  // ---------------------------------------------------------------------------

  /** Retries `attempt` up to `retriesLeft` times with exponential back-off.
    *
    * On `Left` with retries remaining, logs a WARN with the `ref` and remaining retry count (never
    * the error message, which may contain secret content), sleeps `delayMs` milliseconds via
    * `retryDelayFn`, then recurses with `retriesLeft - 1` and `delayMs * 2`. Returns immediately on
    * `Right` or when `retriesLeft == 0`.
    *
    * The method is `@tailrec`-eligible because the recursive call is always in the tail position.
    *
    * @param ref
    *   Original ref — logged on transient failure, never contains secret material.
    * @param attempt
    *   By-name expression that performs one attempt. Re-evaluated on each retry.
    * @param retriesLeft
    *   Remaining retry budget (0 means no further retries after this attempt).
    * @param delayMs
    *   Current back-off delay in milliseconds; doubled on each retry.
    * @return
    *   The first `Right` result, or the last `Left(cause)` when retries are exhausted.
    */
  @tailrec
  private def withRetry(
      ref: String,
      attempt: => Either[String, String],
      retriesLeft: Int,
      delayMs: Long
  ): Either[SecretsResolutionError, String] =
    attempt match {
      case Right(value) =>
        Right(value)
      case Left(_) if retriesLeft <= 0 =>
        Left(SecretsResolutionError(ref, "AWS Secrets Manager call failed"))
      case Left(_) =>
        logger.warn(
          "Transient failure resolving ref: {}; retrying ({} attempts left)",
          ref,
          retriesLeft.toString
        )
        retryDelayFn(delayMs)
        withRetry(ref, attempt, retriesLeft - 1, delayMs * 2)
    }
}
