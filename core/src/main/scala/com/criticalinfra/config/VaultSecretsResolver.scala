package com.criticalinfra.config

import com.fasterxml.jackson.databind.ObjectMapper
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration
import org.slf4j.LoggerFactory
import scala.annotation.tailrec
import scala.util.Try

// =============================================================================
// VaultSecretsResolver — HashiCorp Vault KV v2 implementation of SecretsResolver
//
// Supports two authentication methods (token and AppRole), exponential-backoff
// retry with a configurable delay function, and strict log-safety guarantees:
// secret values, Vault tokens, and AppRole credentials are never written to any
// log output.
// =============================================================================

// -----------------------------------------------------------------------------
// VaultAuthMethod — authentication strategy sealed hierarchy
// -----------------------------------------------------------------------------

/** Sealed hierarchy representing the authentication method used to obtain a Vault client token.
  *
  * Two concrete subtypes are provided:
  *
  *   - [[TokenAuth]] — a pre-issued Vault token is supplied directly.
  *   - [[AppRoleAuth]] — the resolver exchanges a role ID and secret ID for a short-lived client
  *     token via the AppRole login endpoint.
  *
  * The choice of auth method is made at construction time and does not change for the lifetime of
  * the resolver instance.
  */
sealed trait VaultAuthMethod

/** Authentication using a pre-issued Vault token.
  *
  * The token is passed directly as the `X-Vault-Token` header on every secret GET request. No
  * additional login round-trip is performed.
  *
  * @param token
  *   A valid Vault client token (e.g. `"s.XXXXXXXXXXXXXXXXXXXX"`). This value is treated as a
  *   secret and is never written to any log output.
  */
final case class TokenAuth(token: String) extends VaultAuthMethod

/** Authentication using Vault AppRole credentials.
  *
  * On the first call to [[VaultSecretsResolver.resolve]], the resolver posts the `roleId` /
  * `secretId` pair to the AppRole login endpoint and exchanges it for a short-lived `client_token`.
  * That token is then used for the subsequent secret GET request.
  *
  * Both `roleId` and `secretId` are treated as secrets and are never written to any log output.
  *
  * @param roleId
  *   The AppRole role ID (not secret — but still not logged to avoid leaking role topology).
  * @param secretId
  *   The AppRole secret ID. Treated as a credential; never logged.
  */
final case class AppRoleAuth(roleId: String, secretId: String) extends VaultAuthMethod

// -----------------------------------------------------------------------------
// VaultHttpClient — injectable HTTP abstraction for testability
// -----------------------------------------------------------------------------

/** Minimal HTTP abstraction over Vault's REST API, designed for injection in tests.
  *
  * Both methods return `Right(responseBody)` on HTTP 2xx and `Left(errorMessage)` on any non-2xx
  * status code or network-level failure. The response body and error messages never contain secret
  * material — callers must ensure they do not log `Right` values.
  */
trait VaultHttpClient {

  /** Performs an HTTP GET request with the supplied Vault token header.
    *
    * @param url
    *   Fully-qualified URL (e.g. `"http://vault:8200/v1/secret/data/my-path"`).
    * @param token
    *   Vault client token sent as the `X-Vault-Token` request header. Never logged by this method
    *   or its implementations.
    * @return
    *   `Right(body)` on HTTP 2xx, `Left(message)` otherwise.
    */
  def get(url: String, token: String): Either[String, String]

  /** Performs an HTTP POST request with a JSON body.
    *
    * Used exclusively for the AppRole login endpoint. The body will contain `role_id` / `secret_id`
    * values and must never be logged.
    *
    * @param url
    *   Fully-qualified URL (e.g. `"http://vault:8200/v1/auth/approle/login"`).
    * @param jsonBody
    *   JSON-encoded request body. Treated as secret material; never logged.
    * @return
    *   `Right(body)` on HTTP 2xx, `Left(message)` otherwise.
    */
  def post(url: String, jsonBody: String): Either[String, String]
}

/** Production HTTP client backed by [[java.net.http.HttpClient]] (Java 11+).
  *
  * Uses a 10-second connect timeout and a 10-second request timeout. TLS verification uses the
  * JVM's default trust store; no custom trust anchors are configured here.
  */
final class JavaVaultHttpClient extends VaultHttpClient {

  private val client: HttpClient =
    HttpClient
      .newBuilder()
      .connectTimeout(Duration.ofSeconds(10))
      .build()

  /** Sends a GET request to `url` with the `X-Vault-Token` header set to `token`.
    *
    * @param url
    *   Target URL.
    * @param token
    *   Vault token; never logged.
    * @return
    *   `Right(body)` on 2xx, `Left(message)` on non-2xx or IO failure.
    */
  def get(url: String, token: String): Either[String, String] =
    Try {
      val request = HttpRequest
        .newBuilder(URI.create(url))
        .header("X-Vault-Token", token)
        .timeout(Duration.ofSeconds(10))
        .GET()
        .build()
      val response = client.send(request, HttpResponse.BodyHandlers.ofString())
      if (response.statusCode() >= 200 && response.statusCode() < 300)
        Right(response.body())
      else
        Left(s"Vault GET returned HTTP ${response.statusCode()}")
    }.toEither.left.map(e => s"Vault GET network error: ${e.getMessage}").flatten

  /** Sends a POST request to `url` with the given JSON body.
    *
    * @param url
    *   Target URL.
    * @param jsonBody
    *   JSON payload; treated as secret material, never logged.
    * @return
    *   `Right(body)` on 2xx, `Left(message)` on non-2xx or IO failure.
    */
  def post(url: String, jsonBody: String): Either[String, String] =
    Try {
      val request = HttpRequest
        .newBuilder(URI.create(url))
        .header("Content-Type", "application/json")
        .timeout(Duration.ofSeconds(10))
        .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
        .build()
      val response = client.send(request, HttpResponse.BodyHandlers.ofString())
      if (response.statusCode() >= 200 && response.statusCode() < 300)
        Right(response.body())
      else
        Left(s"Vault POST returned HTTP ${response.statusCode()}")
    }.toEither.left.map(e => s"Vault POST network error: ${e.getMessage}").flatten
}

/** Companion object for [[VaultHttpClient]]. */
object VaultHttpClient {

  /** Returns the default production HTTP client backed by [[JavaVaultHttpClient]].
    */
  def default: VaultHttpClient = new JavaVaultHttpClient
}

// -----------------------------------------------------------------------------
// VaultSecretsResolver — main implementation
// -----------------------------------------------------------------------------

/** Production implementation of [[SecretsResolver]] that resolves `vault://`-scheme references
  * against a HashiCorp Vault KV v2 endpoint.
  *
  * ==Usage==
  * {{{
  *   val resolver = new VaultSecretsResolver(
  *     vaultAddress = "https://vault.internal:8200",
  *     authMethod   = TokenAuth(sys.env("VAULT_TOKEN"))
  *   )
  *   resolver.resolve("vault://secret/data/oracle-tradedb-prod")
  *   // => Right("the-actual-password")
  * }}}
  *
  * ==Auth methods==
  * Pass [[TokenAuth]] when a long-lived token is available in the environment. Pass [[AppRoleAuth]]
  * for short-lived, role-scoped tokens in automated deployments (Kubernetes, CI/CD).
  *
  * ==Retry behaviour==
  * Transient failures (non-2xx responses, network errors) are retried up to `maxRetries` times with
  * exponential back-off starting at 500 ms. The delay function is injectable so that unit tests can
  * set it to a no-op.
  *
  * ==Log safety==
  * The resolved secret value, Vault tokens, and AppRole credentials are '''never''' written to any
  * log output at any level. Only the `ref` path and sanitised error messages are logged.
  *
  * ==Thread safety==
  * Instances are safe for concurrent use from multiple Spark tasks. The underlying
  * `JavaVaultHttpClient` builds a new `HttpClient` per instance but each call is stateless.
  *
  * @param vaultAddress
  *   Base URL of the Vault server, without trailing slash (e.g. `"https://vault.internal:8200"`).
  * @param authMethod
  *   Authentication strategy — either [[TokenAuth]] or [[AppRoleAuth]].
  * @param maxRetries
  *   Maximum number of retry attempts after the initial failure. Default: 3.
  * @param httpClient
  *   HTTP client to use. Defaults to [[VaultHttpClient.default]]. Override in tests to inject a
  *   stub.
  * @param retryDelayFn
  *   Function called with the delay in milliseconds before each retry. Defaults to `Thread.sleep`.
  *   Override in tests with `_ => ()` for instant retries.
  */
final class VaultSecretsResolver(
    vaultAddress: String,
    authMethod: VaultAuthMethod,
    maxRetries: Int = 3,
    httpClient: VaultHttpClient = VaultHttpClient.default,
    retryDelayFn: Long => Unit = Thread.sleep
) extends SecretsResolver {

  private val logger = LoggerFactory.getLogger(classOf[VaultSecretsResolver])

  private val mapper = new ObjectMapper()

  private val VaultScheme = "vault://"

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  /** Resolves a `vault://`-scheme reference to its plaintext secret value.
    *
    * Steps:
    *   1. Validates that `ref` starts with `"vault://"`. 2. Strips the scheme prefix to obtain the
    *      KV v2 path. 3. Obtains a Vault client token via the configured auth method. 4. Issues a
    *      GET to `{vaultAddress}/v1/{path}` with the token. 5. Parses `data.data.value` from the KV
    *      v2 JSON response.
    *
    * All HTTP calls (AppRole login and secret GET) are wrapped in retry logic with exponential
    * back-off.
    *
    * @param ref
    *   A `vault://`-prefixed path, e.g. `"vault://secret/data/oracle-tradedb-prod"`.
    * @return
    *   `Right(secret)` on success, `Left(SecretsResolutionError)` on any failure. The resolved
    *   secret value is never logged.
    */
  def resolve(ref: String): Either[SecretsResolutionError, String] = {
    if (!ref.startsWith(VaultScheme)) {
      logger.warn("Secrets ref does not use vault:// scheme: {}", ref)
      return Left(
        SecretsResolutionError(ref, s"ref does not use vault:// scheme")
      )
    }

    val vaultPath = ref.stripPrefix(VaultScheme)

    for {
      token  <- obtainToken(ref)
      secret <- withRetry(ref, fetchSecret(ref, vaultPath, token), maxRetries, 500L)
    } yield secret
  }

  // ---------------------------------------------------------------------------
  // Private: token acquisition
  // ---------------------------------------------------------------------------

  /** Obtains a Vault client token appropriate for the configured auth method.
    *
    * For [[TokenAuth]], returns the token directly (no network call). For [[AppRoleAuth]], performs
    * an AppRole login with retry.
    *
    * @param ref
    *   The original secrets reference, used only in error messages.
    * @return
    *   `Right(token)` or `Left(SecretsResolutionError)`.
    */
  private def obtainToken(ref: String): Either[SecretsResolutionError, String] =
    authMethod match {
      case TokenAuth(token) =>
        Right(token)

      case AppRoleAuth(roleId, secretId) =>
        withRetry(ref, performAppRoleLogin(ref, roleId, secretId), maxRetries, 500L)
    }

  /** Performs a single AppRole login attempt.
    *
    * Never logs `roleId`, `secretId`, or the returned `client_token`.
    *
    * @param ref
    *   Original ref for error-message context.
    * @param roleId
    *   AppRole role ID.
    * @param secretId
    *   AppRole secret ID.
    * @return
    *   `Right(clientToken)` or `Left(SecretsResolutionError)`.
    */
  private def performAppRoleLogin(
      ref: String,
      roleId: String,
      secretId: String
  ): Either[SecretsResolutionError, String] = {
    val loginUrl  = s"$vaultAddress/v1/auth/approle/login"
    val loginBody = s"""{"role_id":"$roleId","secret_id":"$secretId"}"""

    httpClient.post(loginUrl, loginBody) match {
      case Left(err) =>
        logger.warn("AppRole login failed for ref {}: {}", ref, err)
        Left(SecretsResolutionError(ref, s"AppRole login failed: $err"))

      case Right(body) =>
        Try {
          val token = mapper.readTree(body).path("auth").path("client_token").asText(null)
          if (token == null)
            Left(
              SecretsResolutionError(
                ref,
                "AppRole login failed: client_token not found in response"
              )
            )
          else
            Right(token)
        }.toEither.left
          .map(e =>
            SecretsResolutionError(ref, s"AppRole login failed: JSON parse error: ${e.getMessage}")
          )
          .flatten
    }
  }

  // ---------------------------------------------------------------------------
  // Private: secret fetch
  // ---------------------------------------------------------------------------

  /** Performs a single KV v2 secret GET attempt.
    *
    * Never logs the resolved secret value.
    *
    * @param ref
    *   Original ref; logged on failure.
    * @param vaultPath
    *   The Vault path (scheme stripped).
    * @param token
    *   Vault client token; never logged.
    * @return
    *   `Right(secretValue)` or `Left(SecretsResolutionError)`.
    */
  private def fetchSecret(
      ref: String,
      vaultPath: String,
      token: String
  ): Either[SecretsResolutionError, String] = {
    val secretUrl = s"$vaultAddress/v1/$vaultPath"

    httpClient.get(secretUrl, token) match {
      case Left(err) =>
        logger.warn("Vault GET failed for ref {}: {}", ref, err)
        Left(SecretsResolutionError(ref, s"Vault GET failed: $err"))

      case Right(body) =>
        Try {
          val value =
            mapper.readTree(body).path("data").path("data").path("value").asText(null)
          if (value == null) {
            logger.warn("Key 'value' not found in Vault response for ref {}", ref)
            Left(SecretsResolutionError(ref, "key 'value' not found in Vault response"))
          }
          else {
            // NOTE: value is NOT logged — log-safety rule
            logger.debug("Successfully resolved secret for ref {}", ref)
            Right(value)
          }
        }.toEither.left
          .map(e => SecretsResolutionError(ref, s"JSON parse error: ${e.getMessage}"))
          .flatten
    }
  }

  // ---------------------------------------------------------------------------
  // Private: retry with exponential back-off
  // ---------------------------------------------------------------------------

  /** Retries `attempt` up to `retriesLeft` times with exponential back-off.
    *
    * On `Left` and `retriesLeft > 0`, sleeps `delayMs` milliseconds (via `retryDelayFn`) then
    * recurses with `retriesLeft - 1` and `delayMs * 2`. Returns immediately on `Right` or when
    * `retriesLeft == 0`.
    *
    * The method is `@tailrec`-eligible because the only recursive call is in the tail position of
    * each branch.
    *
    * @param ref
    *   Original ref, used only for logging.
    * @param attempt
    *   By-name expression that performs one attempt.
    * @param retriesLeft
    *   Remaining retry budget (0 means no further retries).
    * @param delayMs
    *   Current back-off delay in milliseconds.
    * @return
    *   The first `Right` result, or the last `Left` when retries are exhausted.
    */
  @tailrec
  private def withRetry[A](
      ref: String,
      attempt: => Either[SecretsResolutionError, A],
      retriesLeft: Int,
      delayMs: Long
  ): Either[SecretsResolutionError, A] =
    attempt match {
      case right @ Right(_) =>
        right
      case left @ Left(_) if retriesLeft <= 0 =>
        left
      case Left(err) =>
        logger.warn(
          "Transient failure for ref {} (retries left: {}): {}",
          ref,
          retriesLeft.toString,
          err.cause
        )
        retryDelayFn(delayMs)
        withRetry(ref, attempt, retriesLeft - 1, delayMs * 2)
    }
}
