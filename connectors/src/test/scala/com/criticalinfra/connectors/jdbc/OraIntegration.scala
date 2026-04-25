package com.criticalinfra.connectors.jdbc

import org.scalatest.Tag

/** ScalaTest tag used to mark Oracle integration tests.
  *
  * Tests annotated with this tag require a live Oracle Database instance and the ojdbc11 jar on the
  * JVM classpath. They are excluded from default CI runs via the sbt `testOptions` filter in
  * `build.sbt` and only execute when the environment variable `ORA_INTEGRATION=true` is set.
  *
  * To run Oracle integration tests:
  * {{{
  *   ORA_INTEGRATION=true sbt "project connectors" test
  * }}}
  */
object OraIntegration extends Tag("com.criticalinfra.connectors.jdbc.OraIntegration")
