package com.criticalinfra.config

import java.nio.file.{Files, Path}
import java.nio.charset.StandardCharsets
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

// =============================================================================
// ConfigLoaderSpec — unit test suite for ConfigLoader
//
// Tests cover: valid YAML parsing, secrets resolution, schema validation
// failures (missing required field, invalid enum), credential-in-config
// rejection, file-not-found, and unresolvable credentialsRef.
//
// No Spark context is needed — ConfigLoader is pure JVM.
// =============================================================================

/** Unit tests for [[ConfigLoader]].
  *
  * The [[MockSecretsResolver]] is pre-loaded with the exact `credentialsRef` values present in the
  * two canonical example config files so that happy-path loads succeed end-to-end.
  *
  * Temp files are written with [[java.nio.file.Files.createTempFile]] and contain the minimum
  * schema-valid YAML required to isolate the aspect under test.
  */
class ConfigLoaderSpec extends AnyFlatSpec with Matchers {

  // ---------------------------------------------------------------------------
  // Shared test resolver — pre-loaded with all refs used by the example configs
  // ---------------------------------------------------------------------------

  /** Resolver pre-loaded with credentials for both canonical example configs. */
  private val resolver = new MockSecretsResolver(
    Map(
      "vault://secret/prod/oracle-trades/jdbc-credentials"   -> "resolved-oracle-secret",
      "vault://secret/prod/ev-telemetry/storage-credentials" -> "resolved-ev-secret"
    )
  )

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Writes `content` to a temp `.yaml` file and returns its absolute path string. The file is
    * created in the JVM default temp directory and will be deleted on JVM exit.
    */
  private def writeTempYaml(content: String): String = {
    val tmpFile: Path = Files.createTempFile("config-loader-test-", ".yaml")
    tmpFile.toFile.deleteOnExit()
    Files.write(tmpFile, content.getBytes(StandardCharsets.UTF_8))
    tmpFile.toAbsolutePath.toString
  }

  /** A minimal schema-valid YAML with all required sections populated. Individual tests override
    * specific sections to isolate the failure path under test.
    *
    * Uses `energy` sector + `file` connection so it is self-contained without needing a valid
    * credentialsRef in the shared resolver (callers substitute `credentialsRef` as needed).
    */
  private val minimalValidYaml: String =
    """schemaVersion: "1.0"
metadata:
  sourceId: "test-source-001"
  sourceName: "Test Source"
  sector: "energy"
  owner: "test-team"
  environment: "dev"
connection:
  type: "file"
  credentialsRef: "vault://secret/prod/ev-telemetry/storage-credentials"
  filePath: "/data/test/"
  fileFormat: "csv"
ingestion:
  mode: "full"
schemaEnforcement:
  enabled: false
qualityRules:
  enabled: false
quarantine:
  enabled: false
storage:
  layer: "bronze"
  format: "delta"
  path: "s3://datalake-bronze/test/"
monitoring:
  metricsEnabled: false
audit:
  enabled: false
"""

  // ---------------------------------------------------------------------------
  // (a) Valid YAML configs parse to correct case classes
  // ---------------------------------------------------------------------------

  behavior of "ConfigLoader.load with valid YAML"

  it should "parse financial-services-oracle-trades.yaml to the correct SourceConfig" in {
    val path   = "examples/configs/financial-services-oracle-trades.yaml"
    val result = ConfigLoader.load(path, resolver)

    result shouldBe a[Right[_, _]]

    val config = result.toOption.get

    config.metadata.sector shouldBe Sector.FinancialServices
    config.connection.connectionType shouldBe ConnectionType.Jdbc
    config.ingestion.mode shouldBe IngestionMode.Incremental
    config.storage.layer shouldBe StorageLayer.Bronze
    config.storage.format shouldBe StorageFormat.Delta
    config.audit.retentionDays shouldBe 2555
    // credentialsRef must hold the resolved value, not the vault path
    config.connection.credentialsRef shouldBe "resolved-oracle-secret"
  }

  it should "parse energy-ev-telemetry-csv.yaml to the correct SourceConfig" in {
    val path   = "examples/configs/energy-ev-telemetry-csv.yaml"
    val result = ConfigLoader.load(path, resolver)

    result shouldBe a[Right[_, _]]

    val config = result.toOption.get

    config.metadata.sector shouldBe Sector.Energy
    config.connection.connectionType shouldBe ConnectionType.File
    config.connection.fileFormat shouldBe Some(FileFormat.Csv)
    config.ingestion.mode shouldBe IngestionMode.Full
    // credentialsRef must hold the resolved value, not the vault path
    config.connection.credentialsRef shouldBe "resolved-ev-secret"
  }

  // ---------------------------------------------------------------------------
  // (b) Missing required field returns SchemaValidationError
  // ---------------------------------------------------------------------------

  behavior of "ConfigLoader.load with a missing required field"

  it should "return Left(SchemaValidationError) when the 'audit' section is absent" in {
    // Build the YAML without any audit block at all
    val yamlWithoutAudit =
      """schemaVersion: "1.0"
metadata:
  sourceId: "test-source-001"
  sourceName: "Test Source"
  sector: "energy"
  owner: "test-team"
  environment: "dev"
connection:
  type: "file"
  credentialsRef: "vault://secret/prod/ev-telemetry/storage-credentials"
  filePath: "/data/test/"
  fileFormat: "csv"
ingestion:
  mode: "full"
schemaEnforcement:
  enabled: false
qualityRules:
  enabled: false
quarantine:
  enabled: false
storage:
  layer: "bronze"
  format: "delta"
  path: "s3://datalake-bronze/test/"
monitoring:
  metricsEnabled: false
"""

    val path   = writeTempYaml(yamlWithoutAudit)
    val result = ConfigLoader.load(path, resolver)

    result shouldBe a[Left[_, _]]
    result.swap.toOption.get shouldBe a[SchemaValidationError]

    val err = result.swap.toOption.get.asInstanceOf[SchemaValidationError]
    // The JSON Pointer for a missing top-level key is reported at root or at the key itself.
    // We assert the field string is non-empty and the message is non-empty; exact format
    // is intentionally not asserted so the test survives minor library version upgrades.
    err.message should not be empty
    // Either the field contains "audit" (if networknt 1.4.x names the missing property)
    // or the message text references "audit". At minimum neither should be empty.
    (err.field + err.message) should include("audit")
  }

  // ---------------------------------------------------------------------------
  // (c) Invalid enum value returns SchemaValidationError
  // ---------------------------------------------------------------------------

  behavior of "ConfigLoader.load with an invalid enum value"

  it should "return Left(SchemaValidationError) when connection.type is an unknown value" in {
    val yaml =
      """schemaVersion: "1.0"
metadata:
  sourceId: "test-source-001"
  sourceName: "Test Source"
  sector: "energy"
  owner: "test-team"
  environment: "dev"
connection:
  type: "ftp"
  credentialsRef: "vault://secret/prod/ev-telemetry/storage-credentials"
  filePath: "/data/test/"
  fileFormat: "csv"
ingestion:
  mode: "full"
schemaEnforcement:
  enabled: false
qualityRules:
  enabled: false
quarantine:
  enabled: false
storage:
  layer: "bronze"
  format: "delta"
  path: "s3://datalake-bronze/test/"
monitoring:
  metricsEnabled: false
audit:
  enabled: false
"""

    val path   = writeTempYaml(yaml)
    val result = ConfigLoader.load(path, resolver)

    result shouldBe a[Left[_, _]]
    result.swap.toOption.get shouldBe a[SchemaValidationError]

    val err = result.swap.toOption.get.asInstanceOf[SchemaValidationError]
    err.message should not be empty
  }

  // ---------------------------------------------------------------------------
  // (d) credentialsRef is resolved via MockSecretsResolver (covered by test (a))
  // ---------------------------------------------------------------------------
  // The explicit assertion config.connection.credentialsRef == "resolved-oracle-secret"
  // in test (a) already covers this requirement.

  // ---------------------------------------------------------------------------
  // (e) Literal credential field in connection block returns a Left
  // ---------------------------------------------------------------------------

  behavior of "ConfigLoader.load with a literal credential in the connection block"

  it should "return Left when connection contains a 'password' field (blocked by schema 'not' rule)" in {
    // The JSON Schema has a `not { anyOf: [ {required: ["password"]}, ... ] }` constraint
    // that rejects any connection block containing a plain-text credential field.
    val yaml =
      """schemaVersion: "1.0"
metadata:
  sourceId: "test-source-001"
  sourceName: "Test Source"
  sector: "energy"
  owner: "test-team"
  environment: "dev"
connection:
  type: "file"
  credentialsRef: "vault://secret/prod/ev-telemetry/storage-credentials"
  filePath: "/data/test/"
  fileFormat: "csv"
  password: "secret123"
ingestion:
  mode: "full"
schemaEnforcement:
  enabled: false
qualityRules:
  enabled: false
quarantine:
  enabled: false
storage:
  layer: "bronze"
  format: "delta"
  path: "s3://datalake-bronze/test/"
monitoring:
  metricsEnabled: false
audit:
  enabled: false
"""

    val path   = writeTempYaml(yaml)
    val result = ConfigLoader.load(path, resolver)

    // The schema has `additionalProperties: false` on the connection object, and the `not`
    // rule blocks `password`. Either or both constraints may trigger; what matters is that
    // the load fails.
    result shouldBe a[Left[_, _]]
  }

  // ---------------------------------------------------------------------------
  // (f) File not found returns ParseError
  // ---------------------------------------------------------------------------

  behavior of "ConfigLoader.load with a nonexistent file path"

  it should "return Left(ParseError) mentioning the path when the file does not exist" in {
    val path   = "/nonexistent/path/config.yaml"
    val result = ConfigLoader.load(path, resolver)

    result shouldBe a[Left[_, _]]
    result.swap.toOption.get shouldBe a[ParseError]

    val err = result.swap.toOption.get.asInstanceOf[ParseError]
    err.message should include(path)
  }

  // ---------------------------------------------------------------------------
  // (g) Unresolvable credentialsRef returns SecretsResolutionError
  // ---------------------------------------------------------------------------

  behavior of "ConfigLoader.load with an unresolvable credentialsRef"

  it should "return Left(SecretsResolutionError) with the ref when the resolver does not know it" in {
    val unknownRef = "vault://secret/missing/ref"
    val yaml =
      s"""schemaVersion: "1.0"
metadata:
  sourceId: "test-source-001"
  sourceName: "Test Source"
  sector: "energy"
  owner: "test-team"
  environment: "dev"
connection:
  type: "file"
  credentialsRef: "$unknownRef"
  filePath: "/data/test/"
  fileFormat: "csv"
ingestion:
  mode: "full"
schemaEnforcement:
  enabled: false
qualityRules:
  enabled: false
quarantine:
  enabled: false
storage:
  layer: "bronze"
  format: "delta"
  path: "s3://datalake-bronze/test/"
monitoring:
  metricsEnabled: false
audit:
  enabled: false
"""

    val path   = writeTempYaml(yaml)
    val result = ConfigLoader.load(path, resolver)

    result shouldBe a[Left[_, _]]
    result.swap.toOption.get shouldBe a[SecretsResolutionError]

    val err = result.swap.toOption.get.asInstanceOf[SecretsResolutionError]
    err.ref shouldBe unknownRef
  }

  // ---------------------------------------------------------------------------
  // Additional coverage: S3 URI rejection, unsupported extension, HOCON parse
  // ---------------------------------------------------------------------------

  behavior of "ConfigLoader.load with S3 URIs or unsupported extensions"

  it should "return Left(ParseError) for an S3 URI path" in {
    val result = ConfigLoader.load("s3://bucket/path/config.yaml", resolver)
    result shouldBe a[Left[_, _]]
    result.swap.toOption.get shouldBe a[ParseError]
  }

  it should "return Left(ParseError) for a file with an unsupported extension" in {
    val tmpFile = Files.createTempFile("test-", ".xml")
    tmpFile.toFile.deleteOnExit()
    Files.write(tmpFile, "<config/>".getBytes(StandardCharsets.UTF_8))
    val result = ConfigLoader.load(tmpFile.toAbsolutePath.toString, resolver)
    result shouldBe a[Left[_, _]]
    result.swap.toOption.get shouldBe a[ParseError]
    result.swap.toOption.get.asInstanceOf[ParseError].message should include("xml")
  }

  it should "return Left(ParseError) for malformed YAML" in {
    val tmpFile = Files.createTempFile("test-", ".yaml")
    tmpFile.toFile.deleteOnExit()
    Files.write(tmpFile, "{ bad yaml: [unclosed".getBytes(StandardCharsets.UTF_8))
    val result = ConfigLoader.load(tmpFile.toAbsolutePath.toString, resolver)
    result shouldBe a[Left[_, _]]
    result.swap.toOption.get shouldBe a[ParseError]
  }

  behavior of "ConfigLoader.load with HOCON format"

  it should "return Left(ParseError) for a malformed HOCON file" in {
    val tmpFile = Files.createTempFile("test-", ".conf")
    tmpFile.toFile.deleteOnExit()
    Files.write(tmpFile, "{ unclosed brace without close: [".getBytes(StandardCharsets.UTF_8))
    val result = ConfigLoader.load(tmpFile.toAbsolutePath.toString, resolver)
    result shouldBe a[Left[_, _]]
    result.swap.toOption.get shouldBe a[ParseError]
  }

  it should "parse a minimal valid HOCON config file" in {
    val hocon =
      """schemaVersion = "1.0"
metadata {
  sourceId = "hocon-test-001"
  sourceName = "HOCON Test Source"
  sector = "energy"
  owner = "test-team"
  environment = "dev"
}
connection {
  type = "file"
  credentialsRef = "vault://secret/prod/ev-telemetry/storage-credentials"
  filePath = "/data/test/"
  fileFormat = "csv"
}
ingestion {
  mode = "full"
}
schemaEnforcement {
  enabled = false
}
qualityRules {
  enabled = false
}
quarantine {
  enabled = false
}
storage {
  layer = "bronze"
  format = "delta"
  path = "s3://datalake-bronze/test/"
}
monitoring {
  metricsEnabled = false
}
audit {
  enabled = false
}
"""
    val tmpFile = Files.createTempFile("test-", ".conf")
    tmpFile.toFile.deleteOnExit()
    Files.write(tmpFile, hocon.getBytes(StandardCharsets.UTF_8))
    val result = ConfigLoader.load(tmpFile.toAbsolutePath.toString, resolver)
    result shouldBe a[Right[_, _]]
    val config = result.toOption.get
    config.metadata.sector shouldBe Sector.Energy
    config.connection.credentialsRef shouldBe "resolved-ev-secret"
  }

  behavior of "ConfigLoader.load with optional field defaults"

  it should "apply correct defaults for omitted optional fields" in {
    val path   = writeTempYaml(minimalValidYaml)
    val result = ConfigLoader.load(path, resolver)
    result shouldBe a[Right[_, _]]
    val config = result.toOption.get
    config.ingestion.batchSize shouldBe 10000
    config.ingestion.parallelism shouldBe 4
    config.ingestion.timeout shouldBe 3600
    config.quarantine.retentionDays shouldBe 90
    config.quarantine.errorClassification shouldBe true
    config.storage.compactionEnabled shouldBe false
    config.monitoring.prometheusPort shouldBe 9090
    config.monitoring.alertOnFailure shouldBe true
    config.audit.lineageTracking shouldBe true
    config.audit.immutableRawEnabled shouldBe true
    config.audit.retentionDays shouldBe 2555
  }

  behavior of "ConfigLoader.load with all connection type variants"

  it should "map connection.type 'jdbc' to ConnectionType.Jdbc" in {
    val yaml =
      """schemaVersion: "1.0"
metadata:
  sourceId: "jdbc-test-001"
  sourceName: "JDBC Test"
  sector: "financial-services"
  owner: "team"
  environment: "dev"
connection:
  type: "jdbc"
  credentialsRef: "vault://secret/prod/oracle-trades/jdbc-credentials"
  host: "db.internal"
  port: 1521
  database: "TESTDB"
  jdbcDriver: "oracle"
ingestion:
  mode: "incremental"
  incrementalColumn: "UPDATED_AT"
  watermarkStorage: "s3://state/watermarks/jdbc-test"
schemaEnforcement:
  enabled: false
qualityRules:
  enabled: false
quarantine:
  enabled: false
storage:
  layer: "bronze"
  format: "delta"
  path: "s3://datalake-bronze/test-jdbc/"
monitoring:
  metricsEnabled: false
audit:
  enabled: false
"""
    val path   = writeTempYaml(yaml)
    val result = ConfigLoader.load(path, resolver)
    result shouldBe a[Right[_, _]]
    val config = result.toOption.get
    config.connection.connectionType shouldBe ConnectionType.Jdbc
    config.connection.jdbcDriver shouldBe Some(JdbcDriver.Oracle)
    config.ingestion.mode shouldBe IngestionMode.Incremental
  }

  it should "return Left(SchemaValidationError) for an invalid sector enum value" in {
    val yaml =
      """schemaVersion: "1.0"
metadata:
  sourceId: "test-001"
  sourceName: "Test"
  sector: "aerospace"
  owner: "team"
  environment: "dev"
connection:
  type: "file"
  credentialsRef: "vault://secret/prod/ev-telemetry/storage-credentials"
  filePath: "/data/test/"
  fileFormat: "csv"
ingestion:
  mode: "full"
schemaEnforcement:
  enabled: false
qualityRules:
  enabled: false
quarantine:
  enabled: false
storage:
  layer: "bronze"
  format: "delta"
  path: "s3://datalake-bronze/test/"
monitoring:
  metricsEnabled: false
audit:
  enabled: false
"""
    val path   = writeTempYaml(yaml)
    val result = ConfigLoader.load(path, resolver)
    result shouldBe a[Left[_, _]]
    result.swap.toOption.get shouldBe a[SchemaValidationError]
  }

  it should "return Left(SchemaValidationError) for an invalid storage.format enum value" in {
    val yaml =
      """schemaVersion: "1.0"
metadata:
  sourceId: "test-001"
  sourceName: "Test"
  sector: "energy"
  owner: "team"
  environment: "dev"
connection:
  type: "file"
  credentialsRef: "vault://secret/prod/ev-telemetry/storage-credentials"
  filePath: "/data/test/"
  fileFormat: "csv"
ingestion:
  mode: "full"
schemaEnforcement:
  enabled: false
qualityRules:
  enabled: false
quarantine:
  enabled: false
storage:
  layer: "bronze"
  format: "orc"
  path: "s3://datalake-bronze/test/"
monitoring:
  metricsEnabled: false
audit:
  enabled: false
"""
    val path   = writeTempYaml(yaml)
    val result = ConfigLoader.load(path, resolver)
    result shouldBe a[Left[_, _]]
    result.swap.toOption.get shouldBe a[SchemaValidationError]
  }

  it should "return Left(SchemaValidationError) when schemaVersion is not '1.0'" in {
    val yaml =
      """schemaVersion: "2.0"
metadata:
  sourceId: "test-001"
  sourceName: "Test"
  sector: "energy"
  owner: "team"
  environment: "dev"
connection:
  type: "file"
  credentialsRef: "vault://secret/prod/ev-telemetry/storage-credentials"
  filePath: "/data/test/"
  fileFormat: "csv"
ingestion:
  mode: "full"
schemaEnforcement:
  enabled: false
qualityRules:
  enabled: false
quarantine:
  enabled: false
storage:
  layer: "bronze"
  format: "delta"
  path: "s3://datalake-bronze/test/"
monitoring:
  metricsEnabled: false
audit:
  enabled: false
"""
    val path   = writeTempYaml(yaml)
    val result = ConfigLoader.load(path, resolver)
    result shouldBe a[Left[_, _]]
    result.swap.toOption.get shouldBe a[SchemaValidationError]
  }

  it should "return Left(SchemaValidationError) when qualityRules.enabled is true (const: false)" in {
    val yaml =
      """schemaVersion: "1.0"
metadata:
  sourceId: "test-001"
  sourceName: "Test"
  sector: "energy"
  owner: "team"
  environment: "dev"
connection:
  type: "file"
  credentialsRef: "vault://secret/prod/ev-telemetry/storage-credentials"
  filePath: "/data/test/"
  fileFormat: "csv"
ingestion:
  mode: "full"
schemaEnforcement:
  enabled: false
qualityRules:
  enabled: true
quarantine:
  enabled: false
storage:
  layer: "bronze"
  format: "delta"
  path: "s3://datalake-bronze/test/"
monitoring:
  metricsEnabled: false
audit:
  enabled: false
"""
    val path   = writeTempYaml(yaml)
    val result = ConfigLoader.load(path, resolver)
    result shouldBe a[Left[_, _]]
    result.swap.toOption.get shouldBe a[SchemaValidationError]
  }

  // ---------------------------------------------------------------------------
  // Additional enum coverage: sector variants
  // ---------------------------------------------------------------------------

  behavior of "ConfigLoader.load with sector enum variants"

  /** Resolver extended with refs used by the sector/environment variant tests. */
  private val extendedResolver = new MockSecretsResolver(
    Map(
      "vault://secret/prod/oracle-trades/jdbc-credentials"   -> "resolved-oracle-secret",
      "vault://secret/prod/ev-telemetry/storage-credentials" -> "resolved-ev-secret",
      "vault://secret/dev/test/credentials"                  -> "resolved-dev-secret"
    )
  )

  private def minimalYamlWith(
      sector: String,
      environment: String = "dev",
      credRef: String = "vault://secret/dev/test/credentials"
  ): String =
    s"""schemaVersion: "1.0"
metadata:
  sourceId: "test-001"
  sourceName: "Test"
  sector: "$sector"
  owner: "team"
  environment: "$environment"
connection:
  type: "file"
  credentialsRef: "$credRef"
  filePath: "/data/test/"
  fileFormat: "csv"
ingestion:
  mode: "full"
schemaEnforcement:
  enabled: false
qualityRules:
  enabled: false
quarantine:
  enabled: false
storage:
  layer: "bronze"
  format: "delta"
  path: "s3://datalake-bronze/test/"
monitoring:
  metricsEnabled: false
audit:
  enabled: false
"""

  it should "map sector 'healthcare' to Sector.Healthcare" in {
    val result = ConfigLoader.load(writeTempYaml(minimalYamlWith("healthcare")), extendedResolver)
    result shouldBe a[Right[_, _]]
    result.toOption.get.metadata.sector shouldBe Sector.Healthcare
  }

  it should "map sector 'government' to Sector.Government" in {
    val result = ConfigLoader.load(writeTempYaml(minimalYamlWith("government")), extendedResolver)
    result shouldBe a[Right[_, _]]
    result.toOption.get.metadata.sector shouldBe Sector.Government
  }

  it should "map environment 'staging' to Environment.Staging" in {
    val result =
      ConfigLoader.load(writeTempYaml(minimalYamlWith("energy", "staging")), extendedResolver)
    result shouldBe a[Right[_, _]]
    result.toOption.get.metadata.environment shouldBe Environment.Staging
  }

  it should "map environment 'prod' to Environment.Prod" in {
    val result =
      ConfigLoader.load(writeTempYaml(minimalYamlWith("energy", "prod")), extendedResolver)
    result shouldBe a[Right[_, _]]
    result.toOption.get.metadata.environment shouldBe Environment.Prod
  }

  // ---------------------------------------------------------------------------
  // Additional enum coverage: file format variants
  // ---------------------------------------------------------------------------

  behavior of "ConfigLoader.load with file format enum variants"

  private def minimalYamlWithFileFormat(fmt: String): String =
    s"""schemaVersion: "1.0"
metadata:
  sourceId: "test-001"
  sourceName: "Test"
  sector: "energy"
  owner: "team"
  environment: "dev"
connection:
  type: "file"
  credentialsRef: "vault://secret/dev/test/credentials"
  filePath: "/data/test/"
  fileFormat: "$fmt"
ingestion:
  mode: "full"
schemaEnforcement:
  enabled: false
qualityRules:
  enabled: false
quarantine:
  enabled: false
storage:
  layer: "bronze"
  format: "delta"
  path: "s3://datalake-bronze/test/"
monitoring:
  metricsEnabled: false
audit:
  enabled: false
"""

  it should "map fileFormat 'parquet' to FileFormat.Parquet" in {
    val result =
      ConfigLoader.load(writeTempYaml(minimalYamlWithFileFormat("parquet")), extendedResolver)
    result shouldBe a[Right[_, _]]
    result.toOption.get.connection.fileFormat shouldBe Some(FileFormat.Parquet)
  }

  it should "map fileFormat 'json' to FileFormat.Json" in {
    val result =
      ConfigLoader.load(writeTempYaml(minimalYamlWithFileFormat("json")), extendedResolver)
    result shouldBe a[Right[_, _]]
    result.toOption.get.connection.fileFormat shouldBe Some(FileFormat.Json)
  }

  it should "map fileFormat 'avro' to FileFormat.Avro" in {
    val result =
      ConfigLoader.load(writeTempYaml(minimalYamlWithFileFormat("avro")), extendedResolver)
    result shouldBe a[Right[_, _]]
    result.toOption.get.connection.fileFormat shouldBe Some(FileFormat.Avro)
  }

  // ---------------------------------------------------------------------------
  // Additional enum coverage: storage layer and format variants
  // ---------------------------------------------------------------------------

  behavior of "ConfigLoader.load with storage layer and format variants"

  it should "map storage.layer 'silver' to StorageLayer.Silver" in {
    val yaml   = minimalYamlWith("energy").replace("layer: \"bronze\"", "layer: \"silver\"")
    val result = ConfigLoader.load(writeTempYaml(yaml), extendedResolver)
    result shouldBe a[Right[_, _]]
    result.toOption.get.storage.layer shouldBe StorageLayer.Silver
  }

  it should "map storage.layer 'gold' to StorageLayer.Gold" in {
    val yaml   = minimalYamlWith("energy").replace("layer: \"bronze\"", "layer: \"gold\"")
    val result = ConfigLoader.load(writeTempYaml(yaml), extendedResolver)
    result shouldBe a[Right[_, _]]
    result.toOption.get.storage.layer shouldBe StorageLayer.Gold
  }

  it should "map storage.format 'iceberg' to StorageFormat.Iceberg" in {
    val yaml   = minimalYamlWith("energy").replace("format: \"delta\"", "format: \"iceberg\"")
    val result = ConfigLoader.load(writeTempYaml(yaml), extendedResolver)
    result shouldBe a[Right[_, _]]
    result.toOption.get.storage.format shouldBe StorageFormat.Iceberg
  }

  // ---------------------------------------------------------------------------
  // Additional enum coverage: JDBC driver variants
  // ---------------------------------------------------------------------------

  behavior of "ConfigLoader.load with JDBC driver variants"

  private def jdbcYaml(
      driver: String,
      credRef: String = "vault://secret/dev/test/credentials"
  ): String =
    s"""schemaVersion: "1.0"
metadata:
  sourceId: "jdbc-test-001"
  sourceName: "JDBC Test"
  sector: "financial-services"
  owner: "team"
  environment: "dev"
connection:
  type: "jdbc"
  credentialsRef: "$credRef"
  host: "db.internal"
  port: 5432
  database: "TESTDB"
  jdbcDriver: "$driver"
ingestion:
  mode: "full"
schemaEnforcement:
  enabled: false
qualityRules:
  enabled: false
quarantine:
  enabled: false
storage:
  layer: "bronze"
  format: "delta"
  path: "s3://datalake-bronze/test/"
monitoring:
  metricsEnabled: false
audit:
  enabled: false
"""

  it should "map jdbcDriver 'postgres' to JdbcDriver.Postgres" in {
    val result = ConfigLoader.load(writeTempYaml(jdbcYaml("postgres")), extendedResolver)
    result shouldBe a[Right[_, _]]
    result.toOption.get.connection.jdbcDriver shouldBe Some(JdbcDriver.Postgres)
  }

  it should "map jdbcDriver 'teradata' to JdbcDriver.Teradata" in {
    val result = ConfigLoader.load(writeTempYaml(jdbcYaml("teradata")), extendedResolver)
    result shouldBe a[Right[_, _]]
    result.toOption.get.connection.jdbcDriver shouldBe Some(JdbcDriver.Teradata)
  }

  // ---------------------------------------------------------------------------
  // Additional enum coverage: schemaEnforcement mode permissive
  // ---------------------------------------------------------------------------

  behavior of "ConfigLoader.load with schemaEnforcement mode variants"

  it should "map schemaEnforcement.mode 'permissive' to SchemaEnforcementMode.Permissive" in {
    val yaml =
      """schemaVersion: "1.0"
metadata:
  sourceId: "test-001"
  sourceName: "Test"
  sector: "energy"
  owner: "team"
  environment: "dev"
connection:
  type: "file"
  credentialsRef: "vault://secret/dev/test/credentials"
  filePath: "/data/test/"
  fileFormat: "csv"
ingestion:
  mode: "full"
schemaEnforcement:
  enabled: true
  mode: "permissive"
  registryRef: "schemas/test-v1.json"
qualityRules:
  enabled: false
quarantine:
  enabled: false
storage:
  layer: "bronze"
  format: "delta"
  path: "s3://datalake-bronze/test/"
monitoring:
  metricsEnabled: false
audit:
  enabled: false
"""
    val result = ConfigLoader.load(writeTempYaml(yaml), extendedResolver)
    result shouldBe a[Right[_, _]]
    result.toOption.get.schemaEnforcement.mode shouldBe Some(SchemaEnforcementMode.Permissive)
  }
}
