package com.criticalinfra.connectors

import com.criticalinfra.engine.ConnectorError
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.ListBuffer

// =============================================================================
// RetryPolicySpec — unit tests for RetryPolicy
//
// All tests inject a no-op or spy delayFn so that Thread.sleep is never called.
// ListBuffer spies record the exact millisecond values passed to the delay
// function, allowing precise assertions about the exponential backoff schedule.
// =============================================================================

class RetryPolicySpec extends AnyFlatSpec with Matchers {

  // ---------------------------------------------------------------------------
  // Helper factories
  // ---------------------------------------------------------------------------

  /** A spy delay function that records every delay value it receives. */
  private def spyDelay(): (ListBuffer[Long], Long => Unit) = {
    val recorded = ListBuffer.empty[Long]
    val fn: Long => Unit = ms => recorded += ms
    (recorded, fn)
  }

  /** A no-op delay function — never records anything. */
  private val noDelay: Long => Unit = _ => ()

  private val testError: ConnectorError = ConnectorError("test-source", "transient failure")

  // ---------------------------------------------------------------------------
  // 1. Returns Right immediately when first attempt succeeds (no delays called)
  // ---------------------------------------------------------------------------

  "RetryPolicy.retry" should "return Right immediately when the first attempt succeeds" in {
    val (delays, spy) = spyDelay()

    val result = RetryPolicy.retry(Right("ok"), maxAttempts = 3, delayFn = spy)

    result shouldBe Right("ok")
    delays shouldBe empty
  }

  // ---------------------------------------------------------------------------
  // 2. Retries after a Left and returns Right on the second attempt
  // ---------------------------------------------------------------------------

  it should "retry after a Left and return Right on the second attempt" in {
    val (delays, spy) = spyDelay()
    var callCount     = 0

    val result = RetryPolicy.retry(
      {
        callCount += 1
        if (callCount >= 2) Right("recovered") else Left(testError)
      },
      maxAttempts = 3,
      delayFn = spy
    )

    result shouldBe Right("recovered")
    callCount shouldBe 2
    // One delay between attempt 1 and attempt 2
    delays.length shouldBe 1
  }

  // ---------------------------------------------------------------------------
  // 3. Exhausts all retries and returns the last Left when every attempt fails
  // ---------------------------------------------------------------------------

  it should "exhaust all retries and return the last Left when every attempt fails" in {
    val lastError = ConnectorError("test-source", "persistent failure")
    var callCount = 0

    val result = RetryPolicy.retry(
      {
        callCount += 1
        Left(lastError)
      },
      maxAttempts = 3,
      delayFn = noDelay
    )

    result shouldBe Left(lastError)
    callCount shouldBe 3
  }

  // ---------------------------------------------------------------------------
  // 4. Delay function is called the correct number of times
  // ---------------------------------------------------------------------------

  it should "call the delay function exactly (maxAttempts - 1) times when all attempts fail" in {
    val (delays, spy) = spyDelay()

    RetryPolicy.retry(Left(testError), maxAttempts = 4, delayFn = spy)

    // 4 attempts → 3 delays (before attempts 2, 3, and 4)
    delays.length shouldBe 3
  }

  // ---------------------------------------------------------------------------
  // 5. Delay values follow the exponential backoff sequence
  // ---------------------------------------------------------------------------

  it should "pass delay values following the exponential sequence 100, 200, 400 ms" in {
    val (delays, spy) = spyDelay()

    RetryPolicy.retry(Left(testError), maxAttempts = 4, delayFn = spy)

    delays.toList shouldBe List(100L, 200L, 400L)
  }

  // ---------------------------------------------------------------------------
  // 6. Returns Left(ConnectorError) immediately when maxAttempts = 0 or negative
  // ---------------------------------------------------------------------------

  it should "return Left(ConnectorError) immediately when maxAttempts is 0" in {
    val (delays, spy) = spyDelay()

    val result = RetryPolicy.retry(Right("never reached"), maxAttempts = 0, delayFn = spy)

    result shouldBe Left(ConnectorError("RetryPolicy", "maxAttempts must be >= 1"))
    delays shouldBe empty
  }

  it should "return Left(ConnectorError) immediately when maxAttempts is negative" in {
    val (delays, spy) = spyDelay()

    val result = RetryPolicy.retry(Right("never reached"), maxAttempts = -5, delayFn = spy)

    result shouldBe Left(ConnectorError("RetryPolicy", "maxAttempts must be >= 1"))
    delays shouldBe empty
  }

  // ---------------------------------------------------------------------------
  // 7. maxAttempts = 1 — returns Left without calling delay when the single attempt fails
  // ---------------------------------------------------------------------------

  it should "return Left without calling delay when maxAttempts = 1 and the attempt fails" in {
    val (delays, spy) = spyDelay()

    val result = RetryPolicy.retry(Left(testError), maxAttempts = 1, delayFn = spy)

    result shouldBe Left(testError)
    delays shouldBe empty
  }

  // ---------------------------------------------------------------------------
  // 8. Returns Right on the last allowed attempt (boundary case)
  // ---------------------------------------------------------------------------

  it should "return Right on the last allowed attempt" in {
    val maxAttempts = 5
    var callCount   = 0

    val result = RetryPolicy.retry(
      {
        callCount += 1
        if (callCount >= maxAttempts) Right("success-on-last") else Left(testError)
      },
      maxAttempts = maxAttempts,
      delayFn = noDelay
    )

    result shouldBe Right("success-on-last")
    callCount shouldBe maxAttempts
  }
}
