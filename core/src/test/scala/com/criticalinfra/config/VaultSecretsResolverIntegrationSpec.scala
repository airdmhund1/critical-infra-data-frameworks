package com.criticalinfra.config

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

// =============================================================================
// VaultSecretsResolverIntegrationSpec
//
// Testcontainers-based integration test suite for VaultSecretsResolver.
//
// Requirements:
//   - Docker must be running and the hashicorp/vault:1.17.5 image must be
//     pullable from Docker Hub.
//   - The Vault container is started once in beforeAll() and stopped in
//     afterAll(); all four tests share the same container instance.
//   - A single KV v2 secret ("value" key) is seeded in beforeAll() via the
//     Vault HTTP API using the root dev token.
//   - Test 4 explicitly asserts that the resolved secret value does not appear
//     in any log output captured via a Log4j2 WriterAppender during the
//     resolution call.
// =============================================================================

/** Integration test for [[VaultSecretsResolver]] against a real HashiCorp Vault
  * container managed by Testcontainers.
  *
  * Requires Docker to be running and the `hashicorp/vault:1.17.5` image to be
  * pullable from Docker Hub.
  *
  * The container is started once in `beforeAll()` and stopped in `afterAll()` —
  * all tests share the same container instance.
  *
  * The secret is seeded in `beforeAll()` via the Vault HTTP API using the root
  * dev token (`"dev-root-token"`).
  *
  * Test 4 explicitly asserts that the resolved secret value does not appear in
  * any log output captured during the resolution call. Log capture is
  * implemented using a Log4j2 `WriterAppender` attached dynamically to the
  * `VaultSecretsResolver` logger.
  */
class VaultSecretsResolverIntegrationSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll {

  // ---------------------------------------------------------------------------
  // Constants
  // ---------------------------------------------------------------------------

  private val RootToken   = "dev-root-token"
  private val MountPath   = "secret"
  private val SecretPath  = "test/oracle-prod"
  private val SecretValue = "integration-test-secret-xyz-9f3a"
  private val SecretRef   = s"vault://$MountPath/data/$SecretPath"

  // ---------------------------------------------------------------------------
  // Container state
  // ---------------------------------------------------------------------------

  private var vaultContainer: org.testcontainers.containers.GenericContainer[_] = _
  private var vaultAddress: String = _

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------

  override def beforeAll(): Unit = {
    import org.testcontainers.containers.GenericContainer
    import org.testcontainers.containers.wait.strategy.Wait
    import org.testcontainers.utility.DockerImageName
    import java.time.Duration

    vaultContainer = new GenericContainer(DockerImageName.parse("hashicorp/vault:1.17.5"))
    vaultContainer.withEnv("VAULT_DEV_ROOT_TOKEN_ID", RootToken)
    vaultContainer.withEnv("VAULT_DEV_LISTEN_ADDRESS", "0.0.0.0:8200")
    vaultContainer.withExposedPorts(8200)
    vaultContainer.waitingFor(
      Wait
        .forHttp("/v1/sys/health")
        .forStatusCode(200)
        .withStartupTimeout(Duration.ofSeconds(60))
    )
    vaultContainer.start()

    vaultAddress =
      s"http://${vaultContainer.getHost}:${vaultContainer.getMappedPort(8200)}"
    seedSecret()
  }

  override def afterAll(): Unit =
    if (vaultContainer != null) vaultContainer.stop()

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  /** Seeds the integration-test secret into the running Vault container via a
    * single HTTP PUT to the KV v2 mount.
    *
    * Uses [[java.net.http.HttpClient]] (Java 11+) — the same HTTP stack used by
    * the production code — so no additional test dependency is introduced.
    */
  private def seedSecret(): Unit = {
    import java.net.URI
    import java.net.http.{HttpClient, HttpRequest, HttpResponse}
    import java.time.Duration

    val url  = s"$vaultAddress/v1/$MountPath/data/$SecretPath"
    val body = s"""{"data":{"value":"$SecretValue"}}"""

    val client = HttpClient
      .newBuilder()
      .connectTimeout(Duration.ofSeconds(10))
      .build()

    val request = HttpRequest
      .newBuilder()
      .uri(URI.create(url))
      .header("X-Vault-Token", RootToken)
      .header("Content-Type", "application/json")
      .PUT(HttpRequest.BodyPublishers.ofString(body))
      .timeout(Duration.ofSeconds(10))
      .build()

    val response = client.send(request, HttpResponse.BodyHandlers.ofString())
    require(
      response.statusCode() == 200,
      s"Failed to seed secret at $url: HTTP ${response.statusCode()} — ${response.body()}"
    )
  }

  /** Captures log output emitted by [[VaultSecretsResolver]]'s logger during
    * the execution of `action`.
    *
    * Attaches a Log4j2 [[org.apache.logging.log4j.core.appender.WriterAppender]]
    * to the `VaultSecretsResolver` logger before calling `action`, then removes
    * it in a `finally` block so the global logging configuration is always
    * restored.
    *
    * @param action
    *   By-name expression whose log output is to be captured.
    * @return
    *   A pair of `(actionResult, capturedLogOutput)`.
    */
  private def captureLogsFor[A](action: => A): (A, String) = {
    import org.apache.logging.log4j.LogManager
    import org.apache.logging.log4j.core.LoggerContext
    import org.apache.logging.log4j.core.appender.WriterAppender
    import org.apache.logging.log4j.core.layout.PatternLayout
    import java.io.StringWriter

    val writer       = new StringWriter()
    val layout       = PatternLayout.createDefaultLayout()
    val appenderName = "test-log-capture"
    val appender =
      WriterAppender.createAppender(layout, null, writer, appenderName, false, true)
    appender.start()

    val ctx        = LogManager.getContext(false).asInstanceOf[LoggerContext]
    val config     = ctx.getConfiguration
    val loggerName = classOf[VaultSecretsResolver].getName
    val loggerConf = config.getLoggerConfig(loggerName)
    loggerConf.addAppender(appender, null, null)
    ctx.updateLoggers()

    try {
      val result = action
      (result, writer.toString)
    } finally {
      loggerConf.removeAppender(appenderName)
      ctx.updateLoggers()
      appender.stop()
    }
  }

  // ---------------------------------------------------------------------------
  // Test 1 — happy path: resolve a real secret
  // ---------------------------------------------------------------------------

  "VaultSecretsResolver integration" should
    "resolve a real secret from a live Vault container using token auth" in {
    val resolver = new VaultSecretsResolver(
      vaultAddress = vaultAddress,
      authMethod   = TokenAuth(RootToken),
      maxRetries   = 0,
      retryDelayFn = _ => ()
    )
    val result = resolver.resolve(SecretRef)
    result shouldBe Right(SecretValue)
  }

  // ---------------------------------------------------------------------------
  // Test 2 — non-existent path returns Left
  // ---------------------------------------------------------------------------

  it should "return Left(SecretsResolutionError) for a non-existent secret path" in {
    val resolver = new VaultSecretsResolver(
      vaultAddress = vaultAddress,
      authMethod   = TokenAuth(RootToken),
      maxRetries   = 0,
      retryDelayFn = _ => ()
    )
    val result = resolver.resolve("vault://secret/data/nonexistent/path/xyz")
    result.isLeft shouldBe true
    result.swap.toOption.get.ref shouldBe "vault://secret/data/nonexistent/path/xyz"
  }

  // ---------------------------------------------------------------------------
  // Test 3 — invalid token returns Left
  // ---------------------------------------------------------------------------

  it should "return Left(SecretsResolutionError) when the token is invalid" in {
    val resolver = new VaultSecretsResolver(
      vaultAddress = vaultAddress,
      authMethod   = TokenAuth("invalid-token-xyz"),
      maxRetries   = 0,
      retryDelayFn = _ => ()
    )
    val result = resolver.resolve(SecretRef)
    result.isLeft shouldBe true
  }

  // ---------------------------------------------------------------------------
  // Test 4 — log-safety: secret value must never appear in any log output
  // ---------------------------------------------------------------------------

  it should "never write the resolved secret value to any log output" in {
    val resolver = new VaultSecretsResolver(
      vaultAddress = vaultAddress,
      authMethod   = TokenAuth(RootToken),
      maxRetries   = 0,
      retryDelayFn = _ => ()
    )
    val (result, logOutput) = captureLogsFor(resolver.resolve(SecretRef))
    result shouldBe Right(SecretValue)
    logOutput should not include SecretValue
  }
}
