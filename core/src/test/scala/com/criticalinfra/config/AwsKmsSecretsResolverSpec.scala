package com.criticalinfra.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

// =============================================================================
// AwsKmsSecretsResolverSpec — unit tests for AwsKmsSecretsResolver
//
// All tests use StubAwsSecretsClient to avoid any real AWS calls.
// The retryDelayFn is set to `_ => ()` throughout so tests run at full speed.
// =============================================================================

/** Lightweight stub for [[AwsSecretsClient]] backed by a pre-configured response map.
  *
  * Any secret ID not present in the supplied map returns a `Left` describing the missing stub so
  * that unexpected calls are immediately visible in test output.
  *
  * @param responses
  *   Map from secret ID (without the `aws-secrets://` prefix) to the expected response.
  */
final class StubAwsSecretsClient(
    responses: Map[String, Either[String, String]]
) extends AwsSecretsClient {
  def getSecretValue(secretId: String): Either[String, String] =
    responses.getOrElse(secretId, Left(s"no stub for $secretId"))
}

/** Unit tests for [[AwsKmsSecretsResolver]].
  *
  * Tests cover: happy path, invalid ref scheme, AWS call failure, retry-and-succeed, retry
  * exhaustion, log-safety (secret value must not appear in error output), and correct prefix
  * stripping before the AWS call.
  */
class AwsKmsSecretsResolverSpec extends AnyFlatSpec with Matchers {

  // ---------------------------------------------------------------------------
  // Shared constants used across all test cases
  // ---------------------------------------------------------------------------

  val secretId: String    = "prod/oracle-tradedb/jdbc-credentials"
  val ref: String         = s"aws-secrets://$secretId"
  val secretValue: String = "super-secret-aws-value-456"

  // ---------------------------------------------------------------------------
  // Test 1 — happy path: Right(secret) returned for valid ref and successful AWS call
  // ---------------------------------------------------------------------------

  "AwsKmsSecretsResolver" should "return Right(secret) for happy path" in {
    val stub = new StubAwsSecretsClient(Map(secretId -> Right(secretValue)))
    val resolver = new AwsKmsSecretsResolver(
      awsClient    = stub,
      maxRetries   = 0,
      retryDelayFn = _ => ()
    )

    val result = resolver.resolve(ref)
    result shouldBe Right(secretValue)
  }

  // ---------------------------------------------------------------------------
  // Test 2 — invalid scheme: non-aws-secrets:// ref is rejected immediately
  // ---------------------------------------------------------------------------

  it should "return Left(SecretsResolutionError) for non-aws-secrets:// ref" in {
    val badRef   = "vault://some/path"
    val stub     = new StubAwsSecretsClient(Map.empty)
    val resolver = new AwsKmsSecretsResolver(
      awsClient    = stub,
      maxRetries   = 0,
      retryDelayFn = _ => ()
    )

    val result = resolver.resolve(badRef)

    result.isLeft shouldBe true
    val err = result.swap.toOption.get
    err.ref shouldBe badRef
    err.cause should include("aws-secrets://")
  }

  // ---------------------------------------------------------------------------
  // Test 3 — AWS call failure: Left returned when the AWS client returns Left
  // ---------------------------------------------------------------------------

  it should "return Left(SecretsResolutionError) when AWS call fails" in {
    val stub = new StubAwsSecretsClient(Map(secretId -> Left("ResourceNotFoundException")))
    val resolver = new AwsKmsSecretsResolver(
      awsClient    = stub,
      maxRetries   = 0,
      retryDelayFn = _ => ()
    )

    val result = resolver.resolve(ref)

    result.isLeft shouldBe true
    result.swap.toOption.get.ref shouldBe ref
  }

  // ---------------------------------------------------------------------------
  // Test 4 — retry-and-succeed: first 2 calls fail, third succeeds
  // ---------------------------------------------------------------------------

  it should "retry on transient failure and succeed on third attempt" in {
    var callCount = 0

    val countingStub = new AwsSecretsClient {
      def getSecretValue(id: String): Either[String, String] = {
        callCount += 1
        if (callCount < 3) Left("ServiceUnavailableException")
        else Right(secretValue)
      }
    }

    val resolver = new AwsKmsSecretsResolver(
      awsClient    = countingStub,
      maxRetries   = 3,
      retryDelayFn = _ => ()
    )

    val result = resolver.resolve(ref)
    result shouldBe Right(secretValue)
    callCount shouldBe 3
  }

  // ---------------------------------------------------------------------------
  // Test 5 — retry exhaustion: all attempts fail, Left is returned
  // ---------------------------------------------------------------------------

  it should "exhaust retries and return Left" in {
    val stub = new StubAwsSecretsClient(Map(secretId -> Left("ServiceUnavailableException")))
    val resolver = new AwsKmsSecretsResolver(
      awsClient    = stub,
      maxRetries   = 2,
      retryDelayFn = _ => ()
    )

    val result = resolver.resolve(ref)
    result.isLeft shouldBe true
  }

  // ---------------------------------------------------------------------------
  // Test 6 — log safety: secret value must not appear in any Left error field
  // ---------------------------------------------------------------------------

  it should "never include the secret value in a Left error result" in {
    // The stub returns an error message that embeds the secret value, simulating a misbehaving
    // downstream client.  The resolver must sanitise this before constructing SecretsResolutionError.
    val stub = new StubAwsSecretsClient(
      Map(secretId -> Left(s"error: the-secret-is $secretValue"))
    )
    val resolver = new AwsKmsSecretsResolver(
      awsClient    = stub,
      maxRetries   = 0,
      retryDelayFn = _ => ()
    )

    val result = resolver.resolve(ref)

    result.isLeft shouldBe true
    result.swap.toOption.get.cause should not include secretValue
  }

  // ---------------------------------------------------------------------------
  // Test 7 — prefix stripping: aws-secrets:// is stripped before calling the client
  // ---------------------------------------------------------------------------

  it should "strip aws-secrets:// prefix before calling AWS client" in {
    // The stub only recognises the bare secretId (without the aws-secrets:// scheme prefix).
    // If the resolver passes the full ref to the client the stub returns Left, causing the
    // assertion to fail — proving prefix stripping is performed correctly.
    val stub = new StubAwsSecretsClient(Map(secretId -> Right(secretValue)))
    val resolver = new AwsKmsSecretsResolver(
      awsClient    = stub,
      maxRetries   = 0,
      retryDelayFn = _ => ()
    )

    val result = resolver.resolve(ref)
    result shouldBe Right(secretValue)
  }
}
