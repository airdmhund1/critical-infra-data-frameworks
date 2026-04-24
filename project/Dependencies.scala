import sbt._

/**
 * Central dependency catalogue for the Critical Infrastructure Data Frameworks project.
 *
 * ALL library version strings are defined here. No inline version literals are
 * permitted in build.sbt or any submodule build definition.
 *
 * Grouping:
 *   - Spark       — Apache Spark core and SQL
 *   - Delta       — Delta Lake lakehouse storage
 *   - Config      — Typesafe / Lightbend configuration
 *   - Logging     — SLF4J API (runtime binding provided by Spark)
 *   - Test        — ScalaTest, ScalaCheck, Testcontainers
 */
object Dependencies {

  // ---------------------------------------------------------------------------
  // Version catalogue
  // ---------------------------------------------------------------------------

  object Versions {
    val scala          = "2.13.14"
    val spark          = "3.5.3"
    val delta          = "3.2.1"
    val typesafeConfig = "1.4.3"
    val slf4j          = "2.0.13"
    val scalaTest      = "3.2.19"
    val scalaCheck     = "1.18.0"
    // testcontainers-scala tracks Testcontainers-Java; 0.41.x targets TC-Java 1.19.x
    val testcontainers = "0.41.4"
    val awsSdk         = "2.25.6"
  }

  // ---------------------------------------------------------------------------
  // Spark (provided at runtime in cluster deployments)
  // ---------------------------------------------------------------------------

  val sparkCore: ModuleID =
    "org.apache.spark" %% "spark-core" % Versions.spark % Provided

  val sparkSql: ModuleID =
    "org.apache.spark" %% "spark-sql" % Versions.spark % Provided

  val spark: Seq[ModuleID] = Seq(sparkCore, sparkSql)

  // ---------------------------------------------------------------------------
  // Delta Lake
  // ---------------------------------------------------------------------------

  val deltaSparkCore: ModuleID =
    "io.delta" %% "delta-spark" % Versions.delta % Provided

  /** Delta Lake on the test classpath — required for integration tests that write real Delta tables.
    * This is intentionally separate from `deltaSparkCore` (which is `% Provided`) so that the
    * production dependency scope is never changed.
    */
  val deltaTest: ModuleID =
    "io.delta" %% "delta-spark" % Versions.delta % Test

  val delta: Seq[ModuleID] = Seq(deltaSparkCore)

  val deltaForTest: Seq[ModuleID] = Seq(deltaTest)

  // ---------------------------------------------------------------------------
  // Configuration
  // ---------------------------------------------------------------------------

  val typesafeConfig: ModuleID =
    "com.typesafe" % "config" % Versions.typesafeConfig

  val config: Seq[ModuleID] = Seq(typesafeConfig)

  // ---------------------------------------------------------------------------
  // Logging  (SLF4J API only — Spark supplies the binding)
  // ---------------------------------------------------------------------------

  val slf4jApi: ModuleID =
    "org.slf4j" % "slf4j-api" % Versions.slf4j % Provided

  val logging: Seq[ModuleID] = Seq(slf4jApi)

  // ---------------------------------------------------------------------------
  // Test dependencies
  // ---------------------------------------------------------------------------

  val scalaTest: ModuleID =
    "org.scalatest" %% "scalatest" % Versions.scalaTest % Test

  val scalaCheck: ModuleID =
    "org.scalatestplus" %% "scalacheck-1-18" % "3.2.19.0" % Test

  val testcontainersScala: ModuleID =
    "com.dimafeng" %% "testcontainers-scala-scalatest" % Versions.testcontainers % Test

  val testcontainersJdbc: ModuleID =
    "com.dimafeng" %% "testcontainers-scala-jdbc" % Versions.testcontainers % Test

  // Spark test utilities need spark-core on the test classpath (not Provided)
  val sparkCoreTest: ModuleID =
    "org.apache.spark" %% "spark-core" % Versions.spark % Test

  val sparkSqlTest: ModuleID =
    "org.apache.spark" %% "spark-sql" % Versions.spark % Test

  val test: Seq[ModuleID] = Seq(
    scalaTest,
    scalaCheck,
    testcontainersScala,
    testcontainersJdbc,
    sparkCoreTest,
    sparkSqlTest
  )

  // ---------------------------------------------------------------------------
  // Config loader — YAML parsing and JSON Schema validation
  // ---------------------------------------------------------------------------

  /** SnakeYAML — YAML parsing (explicit declaration; frequently a transitive dep). */
  val snakeYaml: ModuleID =
    "org.yaml" % "snakeyaml" % "2.2"

  /** networknt JSON Schema Validator — Draft 2019-09 compatible schema validation. */
  val jsonSchemaValidator: ModuleID =
    "com.networknt" % "json-schema-validator" % "1.4.3"

  /** Jackson Databind — JSON/YAML node manipulation required by networknt validator. */
  val jacksonDatabind: ModuleID =
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.17.2"

  /** Jackson YAML dataformat — YAML-to-JsonNode conversion via YAMLFactory. */
  val jacksonYaml: ModuleID =
    "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % "2.17.2"

  val configLoader: Seq[ModuleID] = Seq(
    snakeYaml,
    jsonSchemaValidator,
    jacksonDatabind,
    jacksonYaml
  )

  // ---------------------------------------------------------------------------
  // JDBC test utilities — in-process database for connector unit tests
  // ---------------------------------------------------------------------------

  /** H2 in-process JDBC database — used in connector unit tests to exercise JDBC logic without
    * requiring a running external database instance.
    */
  val h2: ModuleID = "com.h2database" % "h2" % "2.2.224" % Test

  val jdbc: Seq[ModuleID] = Seq(h2)

  // ---------------------------------------------------------------------------
  // AWS SDK v2 — Secrets Manager client (production scope; not test-only)
  // ---------------------------------------------------------------------------

  /** AWS SDK v2 Secrets Manager — resolves secrets from AWS Secrets Manager at runtime. */
  val awsSecretsManager: ModuleID =
    "software.amazon.awssdk" % "secretsmanager" % Versions.awsSdk

  /** URL Connection HTTP client for AWS SDK v2 — lightweight alternative to Netty, avoids
    * adding the full Netty stack to the classpath. Required to prevent the default async
    * HTTP client from pulling in netty-all at 10MB+.
    */
  val awsUrlConnectionClient: ModuleID =
    "software.amazon.awssdk" % "url-connection-client" % Versions.awsSdk

  val awsSdk: Seq[ModuleID] = Seq(awsSecretsManager, awsUrlConnectionClient)
}
