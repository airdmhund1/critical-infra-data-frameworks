package com.criticalinfra.config

import org.slf4j.LoggerFactory

import scala.annotation.tailrec

// =============================================================================
// LocalDevSecretsResolver — local-development / CI implementation of SecretsResolver
//
// Reads secrets from JVM system properties via a `local://`-scheme URI.
// Includes a hard CIDF_ENVIRONMENT=production guard, exponential-backoff retry
// with an injectable delay function, and strict log-safety guarantees: resolved
// secret values are never written to any log output.
//
// THIS RESOLVER IS NOT FOR PRODUCTION USE.
// =============================================================================

/** Local-development and CI implementation of [[SecretsResolver]] that resolves `local://`-scheme
  * references from JVM system properties.
  *
  * ==Purpose==
  * This resolver is intended exclusively for local development workstations and CI pipelines where a
  * real secrets manager (HashiCorp Vault, AWS KMS) is not available. Secrets are passed to the
  * process as JVM system properties (e.g. `-Ddb.password=dev-secret`) and looked up by key.
  *
  * ==Production guard==
  * If the environment variable `CIDF_ENVIRONMENT` is set to `"production"` (case-insensitive), this
  * resolver immediately throws an [[IllegalStateException]] rather than returning a `Left`. A
  * misconfigured production deployment is a programming error, not a recoverable runtime failure —
  * hence the throw, which will halt the pipeline loudly rather than silently accepting an insecure
  * configuration. The guard is a safety net; the primary control is ensuring this resolver is never
  * referenced in production configuration.
  *
  * ==Usage==
  * {{{
  *   // Set the system property before calling resolve:
  *   // java -Ddb.password=dev-secret ...
  *   val resolver = new LocalDevSecretsResolver()
  *   resolver.resolve("local://db.password")
  *   // => Right("dev-secret")
  * }}}
  *
  * ==Retry behaviour==
  * Missing properties are retried up to `maxRetries` times with exponential back-off starting at
  * 100 ms. The delay function is injectable so that unit tests can set it to a no-op.
  *
  * ==Log safety==
  * The resolved secret value (the system property value) is '''never''' written to any log output
  * at any level. Only the `ref` path and sanitised, fixed-template messages are logged.
  *
  * ==Thread safety==
  * Instances are safe for concurrent use from multiple Spark tasks. All state is read-only after
  * construction.
  *
  * @param maxRetries
  *   Maximum number of retry attempts after the initial failure. Default: 0 (no retries).
  * @param retryDelayFn
  *   Function called with the delay in milliseconds before each retry. Defaults to `Thread.sleep`.
  *   Override in tests with `_ => ()` for instant retries.
  * @param envReader
  *   Function to read environment variables by name. Defaults to `System.getenv`. Injectable for
  *   testability — `System.getenv` cannot be overridden without forking a JVM, so tests supply a
  *   controlled map-lookup instead.
  * @param propReader
  *   Function to read JVM system properties by name. Defaults to `System.getProperty`. Injectable
  *   for testability — `System.setProperty` pollutes the shared JVM state across tests, so tests
  *   supply a controlled map-lookup instead.
  */
final class LocalDevSecretsResolver(
    maxRetries: Int = 0,
    retryDelayFn: Long => Unit = Thread.sleep,
    envReader: String => Option[String] = key => Option(System.getenv(key)),
    propReader: String => Option[String] = key => Option(System.getProperty(key))
) extends SecretsResolver {

  private val logger = LoggerFactory.getLogger(classOf[LocalDevSecretsResolver])

  private val LocalScheme = "local://"

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  /** Resolves a `local://`-scheme reference to its plaintext value from JVM system properties.
    *
    * Steps (executed in this order):
    *   1. '''Production guard''' — checks `CIDF_ENVIRONMENT` via `envReader`. If the value is
    *      `"production"` (case-insensitive), throws [[IllegalStateException]] immediately, before any
    *      other logic. This is a fail-fast safety net for misconfigured deployments.
    *   2. '''Scheme validation''' — if `ref` does not start with `"local://"`, logs WARN and returns
    *      `Left(SecretsResolutionError)`.
    *   3. '''Key extraction''' — strips the `"local://"` prefix to obtain the system property key
    *      (e.g. `"local://db.password"` → `"db.password"`).
    *   4. '''Resolution with retry''' — looks up the key via `propReader`, retrying up to
    *      `maxRetries` times with exponential back-off on `None`.
    *
    * @param ref
    *   A `local://`-prefixed system property key, e.g. `"local://db.password"`.
    * @return
    *   `Right(secret)` when the system property exists, or `Left(SecretsResolutionError)` if the
    *   scheme is wrong or the property cannot be found after all retries. The resolved value is never
    *   logged.
    * @throws IllegalStateException
    *   if `CIDF_ENVIRONMENT` equals `"production"` (case-insensitive). This exception is
    *   intentional: a misconfigured production deployment is a programming error, not a recoverable
    *   runtime failure.
    */
  def resolve(ref: String): Either[SecretsResolutionError, String] = {

    // Step 1 — Production guard (checked FIRST, before anything else)
    envReader("CIDF_ENVIRONMENT").foreach { value =>
      if (value.trim.equalsIgnoreCase("production")) {
        throw new IllegalStateException(
          "LocalDevSecretsResolver must not be used in production environments. " +
            "Set CIDF_ENVIRONMENT to a non-production value or switch to " +
            "VaultSecretsResolver / AwsKmsSecretsResolver."
        )
      }
    }

    // Step 2 — Scheme validation
    if (!ref.startsWith(LocalScheme)) {
      logger.warn("Secrets ref does not use local:// scheme: {}", ref)
      return Left(SecretsResolutionError(ref, "ref does not use local:// scheme"))
    }

    // Step 3 — Property key extraction
    val key = ref.stripPrefix(LocalScheme)

    // Step 4 — Resolution with retry (by-name so propReader is re-invoked on each attempt)
    withRetry(
      ref,
      propReader(key) match {
        case Some(value) => Right(value)
        case None        => Left(SecretsResolutionError(ref, s"system property '$key' not found"))
      },
      maxRetries,
      100L
    )
  }

  // ---------------------------------------------------------------------------
  // Private: retry with exponential back-off
  // ---------------------------------------------------------------------------

  /** Retries `attempt` up to `retriesLeft` times with exponential back-off.
    *
    * On `Left` and `retriesLeft > 0`, logs a WARN with the `ref` and remaining retry count, sleeps
    * `delayMs` milliseconds via `retryDelayFn`, then recurses with `retriesLeft - 1` and
    * `delayMs * 2`. Returns immediately on `Right` or when `retriesLeft == 0`.
    *
    * The method is `@tailrec`-eligible because the recursive call is always in the tail position.
    *
    * @param ref
    *   Original ref — logged on transient failure; never contains secret material.
    * @param attempt
    *   By-name expression that performs one attempt. Re-evaluated on each retry.
    * @param retriesLeft
    *   Remaining retry budget (0 means no further retries after this attempt).
    * @param delayMs
    *   Current back-off delay in milliseconds; doubled on each retry.
    * @return
    *   The first `Right` result, or the last `Left` when retries are exhausted.
    */
  @tailrec
  private def withRetry(
      ref: String,
      attempt: => Either[SecretsResolutionError, String],
      retriesLeft: Int,
      delayMs: Long
  ): Either[SecretsResolutionError, String] =
    attempt match {
      case right @ Right(_) =>
        right
      case left @ Left(_) if retriesLeft <= 0 =>
        left
      case Left(_) =>
        logger.warn(
          "Property not found for ref: {}; retrying ({} retries left)",
          ref,
          retriesLeft.toString
        )
        retryDelayFn(delayMs)
        withRetry(ref, attempt, retriesLeft - 1, delayMs * 2)
    }
}
