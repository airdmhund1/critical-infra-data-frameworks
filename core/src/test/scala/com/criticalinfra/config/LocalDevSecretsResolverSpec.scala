package com.criticalinfra.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

// =============================================================================
// LocalDevSecretsResolverSpec — unit tests for LocalDevSecretsResolver
//
// All tests use injectable envReader / propReader maps to avoid touching real
// System.getenv / System.setProperty (which would pollute shared JVM state).
// The retryDelayFn is set to `_ => ()` throughout so tests run at full speed.
// =============================================================================

/** Unit tests for [[LocalDevSecretsResolver]].
  *
  * Tests cover: happy-path property resolution, missing property, non-local:// scheme,
  * CIDF_ENVIRONMENT production guard (exact-match and case-insensitive), non-production env
  * passthrough, retry-and-succeed, and retry exhaustion.
  */
class LocalDevSecretsResolverSpec extends AnyFlatSpec with Matchers {

  // ---------------------------------------------------------------------------
  // Shared constants used across all test cases
  // ---------------------------------------------------------------------------

  val propKey   = "db.password"
  val ref       = s"local://$propKey"
  val secretVal = "dev-secret-value-789"

  // ---------------------------------------------------------------------------
  // Helper builder — avoids repetitive constructor boilerplate in each test
  // ---------------------------------------------------------------------------

  /** Builds a [[LocalDevSecretsResolver]] backed by controlled in-memory maps.
    *
    * @param props
    *   System properties to expose to the resolver (defaults to empty).
    * @param env
    *   Environment variables to expose to the resolver (defaults to empty).
    */
  private def resolver(
      props: Map[String, String] = Map.empty,
      env: Map[String, String] = Map.empty
  ): LocalDevSecretsResolver =
    new LocalDevSecretsResolver(
      maxRetries    = 0,
      retryDelayFn  = _ => (),
      envReader     = key => env.get(key),
      propReader    = key => props.get(key)
    )

  // ---------------------------------------------------------------------------
  // Test 1 — happy path: property exists
  // ---------------------------------------------------------------------------

  "LocalDevSecretsResolver" should "return Right(secret) when system property exists" in {
    val result = resolver(props = Map(propKey -> secretVal)).resolve(ref)
    result shouldBe Right(secretVal)
  }

  // ---------------------------------------------------------------------------
  // Test 2 — missing property
  // ---------------------------------------------------------------------------

  it should "return Left(SecretsResolutionError) when property is absent" in {
    val result = resolver().resolve(ref)

    result.isLeft shouldBe true
    val err = result.swap.toOption.get
    err.ref shouldBe ref
    err.cause should include(propKey)
  }

  // ---------------------------------------------------------------------------
  // Test 3 — non-local:// scheme is rejected
  // ---------------------------------------------------------------------------

  it should "return Left(SecretsResolutionError) for non-local:// ref" in {
    val badRef = "vault://some/path"
    val result = resolver().resolve(badRef)

    result.isLeft shouldBe true
    val err = result.swap.toOption.get
    err.ref shouldBe badRef
    err.cause should include("local://")
  }

  // ---------------------------------------------------------------------------
  // Test 4 — production guard: CIDF_ENVIRONMENT=production throws
  // ---------------------------------------------------------------------------

  it should "throw IllegalStateException when CIDF_ENVIRONMENT=production" in {
    val prodResolver = resolver(env = Map("CIDF_ENVIRONMENT" -> "production"))

    val ex = the[IllegalStateException] thrownBy prodResolver.resolve(ref)
    ex.getMessage should include("production")
  }

  // ---------------------------------------------------------------------------
  // Test 5 — production guard: case-insensitive (PRODUCTION)
  // ---------------------------------------------------------------------------

  it should "throw IllegalStateException when CIDF_ENVIRONMENT=PRODUCTION (case-insensitive)" in {
    val prodResolver = resolver(env = Map("CIDF_ENVIRONMENT" -> "PRODUCTION"))

    an[IllegalStateException] should be thrownBy prodResolver.resolve(ref)
  }

  // ---------------------------------------------------------------------------
  // Test 6 — non-production CIDF_ENVIRONMENT is allowed through
  // ---------------------------------------------------------------------------

  it should "resolve successfully when CIDF_ENVIRONMENT is non-production" in {
    val result = resolver(
      props = Map(propKey -> secretVal),
      env   = Map("CIDF_ENVIRONMENT" -> "staging")
    ).resolve(ref)

    result shouldBe Right(secretVal)
  }

  // ---------------------------------------------------------------------------
  // Test 7 — retry: missing on first call, present on second
  // ---------------------------------------------------------------------------

  it should "retry on missing property and succeed when property appears on retry" in {
    var callCount = 0

    val countingPropReader: String => Option[String] = _ => {
      callCount += 1
      if (callCount < 2) None else Some(secretVal)
    }

    val retryResolver = new LocalDevSecretsResolver(
      maxRetries   = 2,
      retryDelayFn = _ => (),
      envReader    = _ => None,
      propReader   = countingPropReader
    )

    val result = retryResolver.resolve(ref)
    result shouldBe Right(secretVal)
    callCount shouldBe 2
  }

  // ---------------------------------------------------------------------------
  // Test 8 — retry exhaustion: property never appears
  // ---------------------------------------------------------------------------

  it should "exhaust retries and return Left when property never appears" in {
    val retryResolver = new LocalDevSecretsResolver(
      maxRetries   = 2,
      retryDelayFn = _ => (),
      envReader    = _ => None,
      propReader   = _ => None
    )

    val result = retryResolver.resolve(ref)
    result.isLeft shouldBe true
  }
}
