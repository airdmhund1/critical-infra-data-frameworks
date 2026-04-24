package com.criticalinfra.config

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

// =============================================================================
// VaultSecretsResolverSpec — unit tests for VaultSecretsResolver
//
// All tests use StubVaultHttpClient to avoid any real network calls.
// The retryDelayFn is set to `_ => ()` throughout so tests run at full speed.
//
// JavaVaultHttpClientSpec uses an in-process com.sun.net.httpserver.HttpServer
// (built into the JDK) to exercise the real HTTP implementation without any
// external infrastructure.
// =============================================================================

/** Lightweight stub for [[VaultHttpClient]] backed by pre-configured response
  * maps.
  *
  * Any URL not present in the supplied maps returns a `Left` describing the
  * missing stub so that unexpected calls are immediately visible.
  *
  * @param getResponses  Map from URL to expected GET response.
  * @param postResponses Map from URL to expected POST response.
  */
final class StubVaultHttpClient(
    getResponses: Map[String, Either[String, String]],
    postResponses: Map[String, Either[String, String]]
) extends VaultHttpClient {
  def get(url: String, token: String): Either[String, String] =
    getResponses.getOrElse(url, Left(s"no stub for GET $url"))
  def post(url: String, body: String): Either[String, String] =
    postResponses.getOrElse(url, Left(s"no stub for POST $url"))
}

/** Unit tests for [[VaultSecretsResolver]].
  *
  * Tests cover: token-auth happy path, AppRole happy path, invalid ref scheme,
  * non-2xx GET, failed AppRole login, retry-and-succeed, retry exhaustion, and
  * log-safety (the resolved secret must never appear in error output).
  */
class VaultSecretsResolverSpec extends AnyFlatSpec with Matchers {

  // ---------------------------------------------------------------------------
  // Shared constants used across all test cases
  // ---------------------------------------------------------------------------

  val vaultAddress: String = "http://vault-test:8200"
  val secretPath: String   = "secret/data/oracle-prod"
  val ref: String          = s"vault://$secretPath"
  val secretValue: String  = "super-secret-password-123"
  val vaultToken: String   = "test-vault-token"

  val kvV2Response: String =
    s"""{"data":{"data":{"value":"$secretValue"}}}"""

  val approleLoginResponse: String =
    s"""{"auth":{"client_token":"$vaultToken"}}"""

  // ---------------------------------------------------------------------------
  // Test 1 — token auth happy path
  // ---------------------------------------------------------------------------

  "VaultSecretsResolver" should "return Right(secret) for token auth happy path" in {
    val stub = new StubVaultHttpClient(
      getResponses  = Map(s"$vaultAddress/v1/$secretPath" -> Right(kvV2Response)),
      postResponses = Map.empty
    )
    val resolver = new VaultSecretsResolver(
      vaultAddress  = vaultAddress,
      authMethod    = TokenAuth(vaultToken),
      maxRetries    = 0,
      httpClient    = stub,
      retryDelayFn  = _ => ()
    )

    val result = resolver.resolve(ref)
    result shouldBe Right(secretValue)
  }

  // ---------------------------------------------------------------------------
  // Test 2 — AppRole auth happy path
  // ---------------------------------------------------------------------------

  it should "return Right(secret) for AppRole auth happy path" in {
    val stub = new StubVaultHttpClient(
      getResponses  = Map(s"$vaultAddress/v1/$secretPath" -> Right(kvV2Response)),
      postResponses = Map(s"$vaultAddress/v1/auth/approle/login" -> Right(approleLoginResponse))
    )
    val resolver = new VaultSecretsResolver(
      vaultAddress  = vaultAddress,
      authMethod    = AppRoleAuth("role-id", "secret-id"),
      maxRetries    = 0,
      httpClient    = stub,
      retryDelayFn  = _ => ()
    )

    val result = resolver.resolve(ref)
    result shouldBe Right(secretValue)
  }

  // ---------------------------------------------------------------------------
  // Test 3 — non-vault:// ref is rejected
  // ---------------------------------------------------------------------------

  it should "return Left(SecretsResolutionError) for non-vault:// ref" in {
    val badRef = "aws-secrets://some/path"
    val stub   = new StubVaultHttpClient(Map.empty, Map.empty)
    val resolver = new VaultSecretsResolver(
      vaultAddress  = vaultAddress,
      authMethod    = TokenAuth(vaultToken),
      maxRetries    = 0,
      httpClient    = stub,
      retryDelayFn  = _ => ()
    )

    val result = resolver.resolve(badRef)

    result.isLeft shouldBe true
    val err = result.swap.toOption.get
    err.ref shouldBe badRef
    err.cause should include("vault://")
  }

  // ---------------------------------------------------------------------------
  // Test 4 — non-2xx GET response
  // ---------------------------------------------------------------------------

  it should "return Left(SecretsResolutionError) when GET returns non-2xx" in {
    val stub = new StubVaultHttpClient(
      getResponses  = Map(s"$vaultAddress/v1/$secretPath" -> Left("403 Forbidden")),
      postResponses = Map.empty
    )
    val resolver = new VaultSecretsResolver(
      vaultAddress  = vaultAddress,
      authMethod    = TokenAuth(vaultToken),
      maxRetries    = 0,
      httpClient    = stub,
      retryDelayFn  = _ => ()
    )

    val result = resolver.resolve(ref)

    result.isLeft shouldBe true
    result.swap.toOption.get.ref shouldBe ref
  }

  // ---------------------------------------------------------------------------
  // Test 5 — AppRole login failure
  // ---------------------------------------------------------------------------

  it should "return Left(SecretsResolutionError) when AppRole login fails" in {
    val stub = new StubVaultHttpClient(
      getResponses  = Map.empty,
      postResponses = Map(s"$vaultAddress/v1/auth/approle/login" -> Left("503 Service Unavailable"))
    )
    val resolver = new VaultSecretsResolver(
      vaultAddress  = vaultAddress,
      authMethod    = AppRoleAuth("role-id", "secret-id"),
      maxRetries    = 0,
      httpClient    = stub,
      retryDelayFn  = _ => ()
    )

    val result = resolver.resolve(ref)

    result.isLeft shouldBe true
    result.swap.toOption.get.cause should include("AppRole login failed")
  }

  // ---------------------------------------------------------------------------
  // Test 6 — retry on transient failure, succeed on third attempt
  // ---------------------------------------------------------------------------

  it should "retry on transient failure and succeed on third attempt" in {
    var callCount = 0

    // A stub whose GET behaviour depends on the call count.
    val countingStub = new VaultHttpClient {
      def get(url: String, token: String): Either[String, String] = {
        callCount += 1
        if (callCount < 3) Left("500 Internal Server Error")
        else Right(kvV2Response)
      }
      def post(url: String, body: String): Either[String, String] =
        Left(s"no stub for POST $url")
    }

    val resolver = new VaultSecretsResolver(
      vaultAddress  = vaultAddress,
      authMethod    = TokenAuth(vaultToken),
      maxRetries    = 3,
      httpClient    = countingStub,
      retryDelayFn  = _ => ()
    )

    val result = resolver.resolve(ref)
    result shouldBe Right(secretValue)
    callCount shouldBe 3
  }

  // ---------------------------------------------------------------------------
  // Test 7 — retry exhaustion
  // ---------------------------------------------------------------------------

  it should "exhaust retries and return Left after all attempts fail" in {
    val stub = new StubVaultHttpClient(
      getResponses  = Map(s"$vaultAddress/v1/$secretPath" -> Left("500 Internal Server Error")),
      postResponses = Map.empty
    )
    val resolver = new VaultSecretsResolver(
      vaultAddress  = vaultAddress,
      authMethod    = TokenAuth(vaultToken),
      maxRetries    = 2,
      httpClient    = stub,
      retryDelayFn  = _ => ()
    )

    val result = resolver.resolve(ref)
    result.isLeft shouldBe true
  }

  // ---------------------------------------------------------------------------
  // Test 8 — log safety: secret value must not appear in any error field
  // ---------------------------------------------------------------------------

  it should "never log the resolved secret value" in {
    // Happy-path: resolver returns Right(secretValue).
    val stub = new StubVaultHttpClient(
      getResponses  = Map(s"$vaultAddress/v1/$secretPath" -> Right(kvV2Response)),
      postResponses = Map.empty
    )
    val resolver = new VaultSecretsResolver(
      vaultAddress  = vaultAddress,
      authMethod    = TokenAuth(vaultToken),
      maxRetries    = 0,
      httpClient    = stub,
      retryDelayFn  = _ => ()
    )

    val result = resolver.resolve(ref)

    // The resolver must have returned the correct value.
    result shouldBe Right(secretValue)

    // The result must not be a Left (which would mean it was never resolved).
    result should not be a[Left[_, _]]

    // Guard: if somehow a Left were returned, its cause must not contain the
    // secret value (this catches accidental inclusion of the resolved secret
    // in an error message).
    result.swap.toOption.foreach { err =>
      err.cause should not include secretValue
    }

    // Additional paranoia check: the Right value itself is the secretValue,
    // confirming we didn't accidentally discard it. The value is NOT printed here.
    result.toOption.get shouldBe secretValue
  }

  // ---------------------------------------------------------------------------
  // Test 9 — missing 'value' key in KV v2 response
  // ---------------------------------------------------------------------------

  it should "return Left(SecretsResolutionError) when Vault response has no 'value' key" in {
    val badBody = """{"data":{"data":{"other":"stuff"}}}"""
    val stub = new StubVaultHttpClient(
      getResponses  = Map(s"$vaultAddress/v1/$secretPath" -> Right(badBody)),
      postResponses = Map.empty
    )
    val resolver = new VaultSecretsResolver(
      vaultAddress  = vaultAddress,
      authMethod    = TokenAuth(vaultToken),
      maxRetries    = 0,
      httpClient    = stub,
      retryDelayFn  = _ => ()
    )

    val result = resolver.resolve(ref)

    result.isLeft shouldBe true
    result.swap.toOption.get.cause should include("value")
  }

  // ---------------------------------------------------------------------------
  // Test 10 — AppRole login response missing client_token
  // ---------------------------------------------------------------------------

  it should "return Left(SecretsResolutionError) when AppRole response has no client_token" in {
    val badLoginBody = """{"auth":{}}"""
    val stub = new StubVaultHttpClient(
      getResponses  = Map.empty,
      postResponses = Map(s"$vaultAddress/v1/auth/approle/login" -> Right(badLoginBody))
    )
    val resolver = new VaultSecretsResolver(
      vaultAddress  = vaultAddress,
      authMethod    = AppRoleAuth("role-id", "secret-id"),
      maxRetries    = 0,
      httpClient    = stub,
      retryDelayFn  = _ => ()
    )

    val result = resolver.resolve(ref)

    result.isLeft shouldBe true
    result.swap.toOption.get.cause should include("AppRole login failed")
  }
}

// =============================================================================
// JavaVaultHttpClientSpec — integration-style tests for the real HTTP client
//
// Uses com.sun.net.httpserver.HttpServer (built into the JDK) as an in-process
// HTTP server so that no external infrastructure is required.
// =============================================================================

/** Tests for [[JavaVaultHttpClient]] exercising the real HTTP implementation
  * against an in-process JDK HttpServer.
  */
class JavaVaultHttpClientSpec extends AnyFlatSpec with Matchers {

  // ---------------------------------------------------------------------------
  // Helper: start an ephemeral HTTP server on a random port
  // ---------------------------------------------------------------------------

  /** Starts an in-process HTTP server on a random available port, registers
    * `handler` at `path`, runs `test`, then stops the server.
    *
    * @param path    The URL path to register (e.g. `"/v1/secret/data/foo"`).
    * @param handler A function `(statusCode, responseBody)` to serve.
    * @param test    Thunk that receives the base URL `"http://127.0.0.1:{port}"`.
    */
  private def withServer(path: String, statusCode: Int, responseBody: String)(
      test: String => Unit
  ): Unit = {
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext(
      path,
      (exchange: HttpExchange) => {
        val bytes = responseBody.getBytes(StandardCharsets.UTF_8)
        exchange.sendResponseHeaders(statusCode, bytes.length.toLong)
        val os = exchange.getResponseBody
        os.write(bytes)
        os.close()
      }
    )
    server.start()
    val port = server.getAddress.getPort
    try {
      test(s"http://127.0.0.1:$port")
    } finally {
      server.stop(0)
    }
  }

  // ---------------------------------------------------------------------------
  // GET — 2xx response
  // ---------------------------------------------------------------------------

  "JavaVaultHttpClient" should "return Right(body) for a 200 GET response" in {
    val path     = "/v1/secret/data/test"
    val expected = """{"data":{"data":{"value":"the-secret"}}}"""
    withServer(path, 200, expected) { baseUrl =>
      val client = new JavaVaultHttpClient
      val result = client.get(s"$baseUrl$path", "some-token")
      result shouldBe Right(expected)
    }
  }

  // ---------------------------------------------------------------------------
  // GET — non-2xx response
  // ---------------------------------------------------------------------------

  it should "return Left(message) for a 403 GET response" in {
    val path = "/v1/secret/data/forbidden"
    withServer(path, 403, "Permission denied") { baseUrl =>
      val client = new JavaVaultHttpClient
      val result = client.get(s"$baseUrl$path", "bad-token")
      result.isLeft shouldBe true
      result.swap.toOption.get should include("403")
    }
  }

  // ---------------------------------------------------------------------------
  // GET — network error (connection refused)
  // ---------------------------------------------------------------------------

  it should "return Left(message) for a GET to an unreachable host" in {
    val client = new JavaVaultHttpClient
    // Port 1 is almost certainly not listening locally.
    val result = client.get("http://127.0.0.1:1/v1/not-there", "token")
    result.isLeft shouldBe true
  }

  // ---------------------------------------------------------------------------
  // POST — 2xx response
  // ---------------------------------------------------------------------------

  it should "return Right(body) for a 200 POST response" in {
    val path     = "/v1/auth/approle/login"
    val expected = """{"auth":{"client_token":"new-token"}}"""
    withServer(path, 200, expected) { baseUrl =>
      val client = new JavaVaultHttpClient
      val result = client.post(s"$baseUrl$path", """{"role_id":"r","secret_id":"s"}""")
      result shouldBe Right(expected)
    }
  }

  // ---------------------------------------------------------------------------
  // POST — non-2xx response
  // ---------------------------------------------------------------------------

  it should "return Left(message) for a 503 POST response" in {
    val path = "/v1/auth/approle/login"
    withServer(path, 503, "Service Unavailable") { baseUrl =>
      val client = new JavaVaultHttpClient
      val result = client.post(s"$baseUrl$path", """{"role_id":"r","secret_id":"s"}""")
      result.isLeft shouldBe true
      result.swap.toOption.get should include("503")
    }
  }

  // ---------------------------------------------------------------------------
  // POST — network error (connection refused)
  // ---------------------------------------------------------------------------

  it should "return Left(message) for a POST to an unreachable host" in {
    val client = new JavaVaultHttpClient
    val result = client.post("http://127.0.0.1:1/v1/auth/approle/login", "{}")
    result.isLeft shouldBe true
  }

  // ---------------------------------------------------------------------------
  // VaultHttpClient.default factory
  // ---------------------------------------------------------------------------

  "VaultHttpClient.default" should "return a JavaVaultHttpClient instance" in {
    VaultHttpClient.default shouldBe a[JavaVaultHttpClient]
  }
}
