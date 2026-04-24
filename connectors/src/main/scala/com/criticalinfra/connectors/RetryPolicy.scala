package com.criticalinfra.connectors

import com.criticalinfra.engine.ConnectorError

import scala.annotation.tailrec

// =============================================================================
// RetryPolicy — exponential-backoff retry utility for source connectors
//
// All JDBC and file connectors delegate transient-failure retries to this
// object rather than implementing ad-hoc retry loops.  The delay function is
// injectable so that unit tests never call Thread.sleep.
// =============================================================================

/** Stateless retry utility with configurable exponential backoff.
  *
  * Each connector that can encounter transient failures (e.g. network blips on a JDBC connection,
  * temporary file-system unavailability) should route those failures through [[RetryPolicy.retry]]
  * rather than implementing a bespoke retry loop.
  *
  * ==Backoff schedule==
  * {{{
  *   Attempt 1 : no delay          (first try)
  *   Attempt 2 : 100 ms            (baseDelayMs * 2^0)
  *   Attempt 3 : 200 ms            (baseDelayMs * 2^1)
  *   Attempt 4 : 400 ms            (baseDelayMs * 2^2)
  *   Attempt N : 100 * 2^(N-2) ms
  * }}}
  *
  * ==Error handling==
  * If all attempts are exhausted the last [[ConnectorError]] received is returned as-is. No
  * exceptions are thrown and no errors are swallowed.
  */
object RetryPolicy {

  /** Base delay in milliseconds applied before the first retry (second attempt). */
  private val baseDelayMs: Long = 100L

  /** Retry `attempt` up to `maxAttempts` times with exponential backoff between retries.
    *
    * @param attempt
    *   By-name expression that produces either a successful result or a [[ConnectorError]]. It is
    *   re-evaluated on every attempt so that side-effectful operations (e.g. opening a JDBC
    *   connection) are retried correctly.
    * @param maxAttempts
    *   Total number of times `attempt` will be evaluated. Must be >= 1; passing 0 or a negative
    *   value returns `Left(ConnectorError("RetryPolicy", "maxAttempts must be >= 1"))` immediately.
    * @param delayFn
    *   Function that receives the delay in milliseconds and performs the actual sleep. Defaults to
    *   [[Thread.sleep]]. Inject a no-op (`_ => ()`) in unit tests to avoid real delays.
    * @tparam A
    *   Type of the successful result.
    * @return
    *   `Right(a)` on the first successful attempt, or the last `Left(ConnectorError)` if every
    *   attempt fails.
    */
  def retry[A](
      attempt: => Either[ConnectorError, A],
      maxAttempts: Int,
      delayFn: Long => Unit = Thread.sleep
  ): Either[ConnectorError, A] =
    if (maxAttempts <= 0)
      Left(ConnectorError("RetryPolicy", "maxAttempts must be >= 1"))
    else
      loop(attempt, maxAttempts, attemptNumber = 1, delayFn)

  /** Tail-recursive inner loop that tracks the current attempt number.
    *
    * @param attempt
    *   By-name expression re-evaluated each call.
    * @param remaining
    *   Number of attempts still permitted (counts down to 0).
    * @param attemptNumber
    *   1-based index of the current attempt, used to compute the backoff delay.
    * @param delayFn
    *   Injectable sleep function.
    */
  @tailrec
  private def loop[A](
      attempt: => Either[ConnectorError, A],
      remaining: Int,
      attemptNumber: Int,
      delayFn: Long => Unit
  ): Either[ConnectorError, A] = {
    val result = attempt
    result match {
      case Right(_)                  => result
      case Left(_) if remaining <= 1 => result
      case Left(_)                   =>
        // Apply delay before the next attempt: baseDelayMs * 2^(attemptNumber - 1)
        val delayMs = baseDelayMs * math.pow(2, attemptNumber - 1).toLong
        delayFn(delayMs)
        loop(attempt, remaining - 1, attemptNumber + 1, delayFn)
    }
  }
}
