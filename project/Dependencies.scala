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

  val delta: Seq[ModuleID] = Seq(deltaSparkCore)

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
}
