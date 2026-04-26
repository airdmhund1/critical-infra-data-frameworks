package com.criticalinfra.connectors.file

import com.criticalinfra.config.{
  Audit,
  Connection,
  ConnectionType,
  Environment,
  FileFormat,
  Ingestion,
  IngestionMode,
  Metadata,
  Monitoring,
  QualityRules,
  Quarantine,
  SchemaEnforcement,
  SchemaEnforcementMode,
  Sector,
  SourceConfig,
  Storage,
  StorageFormat,
  StorageLayer
}
import com.criticalinfra.engine.ConnectorError
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.PrintWriter
import java.nio.file.Files

// =============================================================================
// JsonFileConnectorSpec — unit tests for JsonFileConnector
//
// Uses temp files written to java.io.tmpdir so that Spark reads from the local
// filesystem with no external dependencies. SparkSession runs in local mode.
//
// Test scenarios (22 total):
//   - Path validation (1)
//   - SchemaRef validation — ADR-005 (1)
//   - Schema loader failure (1)
//   - Strict mode — JSON Lines happy path row count (1)
//   - Strict mode — JSON Lines schema applied correctly (1)
//   - Strict mode — FAILFAST with malformed line (1)
//   - Strict mode — DROPMALFORMED drops bad row (1)
//   - Strict mode — PERMISSIVE returns all rows with null fields (1)
//   - DiscoveredAndLog — schema matches, output schema equals registered (1)
//   - DiscoveredAndLog — extra field in JSON stripped from output (1)
//   - DiscoveredAndLog — ADR-005: inferred schema never leaks into output (1)
//   - DiscoveredAndLog — missing field in JSON (drift detected) (1)
//   - DiscoveredAndLog — type mismatch (drift detected) (1)
//   - MultilineArray format — reads valid JSON array correctly (1)
//   - MultilineArray format — multiLine=true reads array as multiple rows (1)
//   - Flatten depth 0 — nested struct column preserved as StructType (1)
//   - Flatten depth 1 — struct expanded to parent_child top-level columns (1)
//   - Flatten depth 1 — non-struct columns pass through unchanged (1)
//   - Flatten depth 1 — inner nested struct remains a StructType (1)
//   - Flatten depth > 1 — treated as depth 1 (1)
//   - CorruptRecord PERMISSIVE — bad JSON row has null fields (1)
//   - CorruptRecord DROPMALFORMED — bad row excluded, count correct (1)
// =============================================================================

/** Unit test suite for [[JsonFileConnector]].
  *
  * Every test writes JSON content to a temporary file on the local filesystem. SparkSession runs in
  * `local[1]` mode with the UI disabled and shuffle partitions set to 1. No Testcontainers or
  * network connectivity is required.
  */
class JsonFileConnectorSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  // ---------------------------------------------------------------------------
  // SparkSession lifecycle
  // ---------------------------------------------------------------------------

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("JsonFileConnectorSpec")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) spark.stop()
    } finally {
      super.afterAll()
    }
  }

  // ---------------------------------------------------------------------------
  // Declared schemas used across test scenarios
  // ---------------------------------------------------------------------------

  private val testSchema = StructType(
    Seq(
      StructField("id",     LongType,   nullable = true),
      StructField("name",   StringType, nullable = true),
      StructField("amount", DoubleType, nullable = true)
    )
  )

  // Schema with a nested struct field for flattening tests
  private val nestedSchema = StructType(
    Seq(
      StructField("id",   LongType, nullable = true),
      StructField("event", StructType(Seq(
        StructField("type", StringType, nullable = true),
        StructField("ts",   LongType,   nullable = true)
      )), nullable = true)
    )
  )

  // Schema with a two-level nested struct (to test depth-1-only flattening)
  private val deeplyNestedSchema = StructType(
    Seq(
      StructField("id", LongType, nullable = true),
      StructField("outer", StructType(Seq(
        StructField("inner", StructType(Seq(
          StructField("val", StringType, nullable = true)
        )), nullable = true)
      )), nullable = true)
    )
  )

  // ---------------------------------------------------------------------------
  // Schema loader test doubles
  // ---------------------------------------------------------------------------

  private def makeLoader(schema: StructType): String => Either[ConnectorError, StructType] =
    _ => Right(schema)

  private val failLoader: String => Either[ConnectorError, StructType] =
    ref => Left(ConnectorError("test", s"Schema not found: $ref"))

  // ---------------------------------------------------------------------------
  // Config builder helpers
  // ---------------------------------------------------------------------------

  private def makeConfig(
      filePath: Option[String],
      schemaRef: Option[String] = Some("test-ref")
  ): SourceConfig =
    SourceConfig(
      schemaVersion = "1.0",
      metadata = Metadata(
        sourceId    = "test-json-source",
        sourceName  = "Test JSON Source",
        sector      = Sector.Energy,
        owner       = "test-team",
        environment = Environment.Dev
      ),
      connection = Connection(
        connectionType = ConnectionType.File,
        credentialsRef = "local/test",
        filePath       = filePath,
        fileFormat     = Some(FileFormat.Json)
      ),
      ingestion = Ingestion(mode = IngestionMode.Full),
      schemaEnforcement = SchemaEnforcement(
        enabled     = true,
        mode        = Some(SchemaEnforcementMode.Strict),
        registryRef = schemaRef
      ),
      qualityRules = QualityRules(),
      quarantine   = Quarantine(enabled = false),
      storage = Storage(
        layer  = StorageLayer.Bronze,
        format = StorageFormat.Delta,
        path   = "/tmp/test-bronze"
      ),
      monitoring = Monitoring(metricsEnabled = false),
      audit      = Audit(enabled = false)
    )

  // ---------------------------------------------------------------------------
  // Temp file helpers
  // ---------------------------------------------------------------------------

  /** Writes JSON Lines rows (one JSON object per line) to a temp file and returns its path. */
  private def writeJsonLines(rows: Seq[String]): String = {
    val path = Files.createTempFile("json-connector-", ".jsonl")
    path.toFile.deleteOnExit()
    val pw = new PrintWriter(path.toFile)
    try rows.foreach(pw.println)
    finally pw.close()
    path.toAbsolutePath.toString
  }

  /** Writes a multiline JSON document (e.g. a JSON array) to a temp file and returns its path. */
  private def writeMultilineJson(content: String): String = {
    val path = Files.createTempFile("json-connector-", ".json")
    path.toFile.deleteOnExit()
    val pw = new PrintWriter(path.toFile)
    try pw.print(content)
    finally pw.close()
    path.toAbsolutePath.toString
  }

  // ---------------------------------------------------------------------------
  // Standard valid JSON Lines rows matching testSchema
  // ---------------------------------------------------------------------------

  private val validJsonLines = Seq(
    """{"id": 1, "name": "Alice", "amount": 10.5}""",
    """{"id": 2, "name": "Bob",   "amount": 20.0}"""
  )

  // ---------------------------------------------------------------------------
  // Default strict connector factory
  // ---------------------------------------------------------------------------

  private def strictConnector(
      corruptRecordMode: JsonFileConnector.CorruptRecordMode =
        JsonFileConnector.CorruptRecordMode.FailFast,
      loader: String => Either[ConnectorError, StructType] = makeLoader(testSchema)
  ): JsonFileConnector =
    new JsonFileConnector(
      schemaMode        = JsonFileConnector.SchemaMode.Strict,
      corruptRecordMode = corruptRecordMode,
      schemaLoader      = loader
    )

  private def discoveredConnector(
      loader: String => Either[ConnectorError, StructType] = makeLoader(testSchema)
  ): JsonFileConnector =
    new JsonFileConnector(
      schemaMode   = JsonFileConnector.SchemaMode.DiscoveredAndLog,
      schemaLoader = loader
    )

  // ===========================================================================
  // 1. Path validation
  // ===========================================================================

  "JsonFileConnector" should "return Left(ConnectorError) when filePath is absent" in {
    val connector = strictConnector()
    val config    = makeConfig(filePath = None)

    val result = connector.extract(config, spark)

    result shouldBe a[Left[_, _]]
    result.left.map(_.cause should include("filePath")).left.getOrElse(())
  }

  // ===========================================================================
  // 2. SchemaRef validation — ADR-005
  // ===========================================================================

  it should "return Left(ConnectorError) mentioning ADR-005 when schemaRef is absent" in {
    val path      = writeJsonLines(validJsonLines)
    val connector = strictConnector()
    val config    = makeConfig(filePath = Some(path), schemaRef = None)

    val result = connector.extract(config, spark)

    result shouldBe a[Left[_, _]]
    result.left.map(_.cause should include("ADR-005")).left.getOrElse(())
  }

  // ===========================================================================
  // 3. Schema loader failure
  // ===========================================================================

  it should "propagate Left(ConnectorError) when the schema loader fails" in {
    val path      = writeJsonLines(validJsonLines)
    val connector = strictConnector(loader = failLoader)
    val config    = makeConfig(filePath = Some(path))

    val result = connector.extract(config, spark)

    result shouldBe a[Left[_, _]]
    result.left.map(_.cause should include("Schema not found")).left.getOrElse(())
  }

  // ===========================================================================
  // 4. Strict mode — JSON Lines happy path: correct row count
  // ===========================================================================

  it should "return Right(DataFrame) with the correct row count in strict mode" in {
    val path      = writeJsonLines(validJsonLines)
    val connector = strictConnector()
    val config    = makeConfig(filePath = Some(path))

    val result = connector.extract(config, spark)

    result shouldBe a[Right[_, _]]
    result.foreach(df => df.count() shouldBe 2)
  }

  // ===========================================================================
  // 5. Strict mode — correct schema column names and types
  // ===========================================================================

  it should "apply the declared schema column names and types in strict mode" in {
    val path      = writeJsonLines(validJsonLines)
    val connector = strictConnector()
    val config    = makeConfig(filePath = Some(path))

    val result = connector.extract(config, spark)

    result shouldBe a[Right[_, _]]
    result.foreach { df =>
      df.schema.fieldNames.toSeq shouldBe Seq("id", "name", "amount")
      df.schema("id").dataType     shouldBe LongType
      df.schema("name").dataType   shouldBe StringType
      df.schema("amount").dataType shouldBe DoubleType
    }
  }

  // ===========================================================================
  // 6. Strict mode — FAILFAST with a malformed JSON line
  // ===========================================================================

  it should "return Left(ConnectorError) in FAILFAST mode when data contains a malformed JSON line" in {
    // In FAILFAST mode Spark raises an exception when it encounters a malformed record.
    // The connector wraps this in a Left(ConnectorError).
    val path = writeJsonLines(Seq(
      """{"id": 1, "name": "Alice", "amount": 10.5}""",
      """NOT VALID JSON AT ALL""",
      """{"id": 2, "name": "Bob", "amount": 20.0}"""
    ))
    val connector = strictConnector(corruptRecordMode = JsonFileConnector.CorruptRecordMode.FailFast)
    val config    = makeConfig(filePath = Some(path))

    val result = connector.extract(config, spark)

    // Spark JSON FAILFAST raises at action time; the connector returns Right from extract
    // (since the DataFrame is lazy), but the result should be either a Left from a read
    // error or a Right where count() fails. We test the extract-level behaviour:
    // if Right, calling count() should throw — but we only verify the connector
    // does not silently succeed at the extract call level.
    // In practice Spark JSON FAILFAST aborts at action time, so we verify both arms.
    result match {
      case Left(err)  => err.cause should not be empty
      case Right(df)  =>
        // FAILFAST aborts at the action; count() should throw
        an[Exception] should be thrownBy df.count()
    }
  }

  // ===========================================================================
  // 7. Strict mode — DROPMALFORMED: 1 bad line + 2 good lines → 2 rows
  // ===========================================================================

  it should "return Right(DataFrame) with only parseable rows in DROPMALFORMED mode" in {
    val path = writeJsonLines(Seq(
      """{"id": 1, "name": "Alice", "amount": 10.5}""",
      """NOT VALID JSON AT ALL""",
      """{"id": 2, "name": "Bob", "amount": 20.0}"""
    ))
    val connector = new JsonFileConnector(
      schemaMode        = JsonFileConnector.SchemaMode.Strict,
      corruptRecordMode = JsonFileConnector.CorruptRecordMode.DropMalformed,
      schemaLoader      = makeLoader(testSchema)
    )
    val config = makeConfig(filePath = Some(path))

    val result = connector.extract(config, spark)

    result shouldBe a[Right[_, _]]
    result.foreach { df =>
      df.schema.fieldNames.toSeq shouldBe testSchema.fieldNames.toSeq
      df.count() shouldBe 2
    }
  }

  // ===========================================================================
  // 8. Strict mode — PERMISSIVE: bad line returns row with null fields
  // ===========================================================================

  it should "return Right(DataFrame) with all rows in PERMISSIVE mode, nulling un-parseable fields" in {
    val path = writeJsonLines(Seq(
      """{"id": 1, "name": "Alice", "amount": 10.5}""",
      """NOT VALID JSON AT ALL""",
      """{"id": 2, "name": "Bob", "amount": 20.0}"""
    ))
    val connector = new JsonFileConnector(
      schemaMode        = JsonFileConnector.SchemaMode.Strict,
      corruptRecordMode = JsonFileConnector.CorruptRecordMode.Permissive,
      schemaLoader      = makeLoader(testSchema)
    )
    val config = makeConfig(filePath = Some(path))

    val result = connector.extract(config, spark)

    result shouldBe a[Right[_, _]]
    result.foreach { df =>
      // All 3 rows are returned in PERMISSIVE mode; corrupt row has null typed fields
      df.count() shouldBe 3
      // At least one row has a null 'id'
      df.filter(df("id").isNull).count() shouldBe 1L
    }
  }

  // ===========================================================================
  // 9. DiscoveredAndLog — schema matches, output schema equals registered schema
  // ===========================================================================

  it should "return Right(DataFrame) whose schema equals the registered schema in discovered-and-log mode (no drift)" in {
    val path      = writeJsonLines(validJsonLines)
    val connector = discoveredConnector()
    val config    = makeConfig(filePath = Some(path))

    val result = connector.extract(config, spark)

    result shouldBe a[Right[_, _]]
    result.foreach { df =>
      df.count() shouldBe 2
      df.schema.fieldNames.toSeq shouldBe testSchema.fieldNames.toSeq
    }
  }

  // ===========================================================================
  // 10. DiscoveredAndLog — extra field in JSON stripped from output
  // ===========================================================================

  it should "succeed and strip extra fields when discovered-and-log detects an extra JSON field" in {
    // File has an extra 'region' field not in the registered schema
    val path = writeJsonLines(Seq(
      """{"id": 1, "name": "Alice", "amount": 10.5, "region": "West"}""",
      """{"id": 2, "name": "Bob",   "amount": 20.0, "region": "East"}"""
    ))
    val connector = discoveredConnector()
    val config    = makeConfig(filePath = Some(path))

    val result = connector.extract(config, spark)

    result shouldBe a[Right[_, _]]
    result.foreach { df =>
      // Output uses registered schema — extra field is absent
      df.schema.fieldNames should not contain "region"
      df.schema.fieldNames.toSeq shouldBe testSchema.fieldNames.toSeq
    }
  }

  // ===========================================================================
  // 11. DiscoveredAndLog — ADR-005: inferred schema never leaks into output
  // ===========================================================================

  it should "ensure the output DataFrame schema equals the declared schema, not the inferred schema (ADR-005)" in {
    // We declare a registered schema where 'id' and 'amount' are StringType.
    // Spark would infer LongType / DoubleType. The output must carry StringType.
    val allStringSchema = StructType(
      Seq(
        StructField("id",     StringType, nullable = true),
        StructField("name",   StringType, nullable = true),
        StructField("amount", StringType, nullable = true)
      )
    )
    val path      = writeJsonLines(validJsonLines)
    val connector = discoveredConnector(loader = makeLoader(allStringSchema))
    val config    = makeConfig(filePath = Some(path))

    val result = connector.extract(config, spark)

    result shouldBe a[Right[_, _]]
    result.foreach { df =>
      // If inferred schema leaked, 'id' would be LongType and 'amount' would be DoubleType
      df.schema("id").dataType     shouldBe StringType
      df.schema("amount").dataType shouldBe StringType
      // This is the ADR-005 invariant: output schema = registered schema, not inferred
      df.schema.fields.map(_.dataType) shouldBe allStringSchema.fields.map(_.dataType)
    }
  }

  // ===========================================================================
  // 12. DiscoveredAndLog — missing field in JSON → drift logged, registered schema applied
  // ===========================================================================

  it should "succeed and fill missing JSON field with nulls when discovered-and-log detects it" in {
    // File is missing the 'amount' field
    val path = writeJsonLines(Seq(
      """{"id": 1, "name": "Alice"}""",
      """{"id": 2, "name": "Bob"}"""
    ))
    val connector = discoveredConnector()
    val config    = makeConfig(filePath = Some(path))

    val result = connector.extract(config, spark)

    result shouldBe a[Right[_, _]]
    result.foreach { df =>
      // Registered schema is applied — 'amount' column exists (as nulls)
      df.schema.fieldNames should contain("amount")
      df.count() shouldBe 2
      df.filter(df("amount").isNull).count() shouldBe 2
    }
  }

  // ===========================================================================
  // 13. DiscoveredAndLog — type mismatch: WARN logged, registered schema applied
  // ===========================================================================

  it should "succeed and apply registered schema when discovered-and-log detects a type mismatch" in {
    // Registered schema declares 'id' as StringType; Spark would infer LongType
    val stringIdSchema = StructType(
      Seq(
        StructField("id",     StringType, nullable = true),
        StructField("name",   StringType, nullable = true),
        StructField("amount", DoubleType, nullable = true)
      )
    )
    val path      = writeJsonLines(validJsonLines)
    val connector = discoveredConnector(loader = makeLoader(stringIdSchema))
    val config    = makeConfig(filePath = Some(path))

    val result = connector.extract(config, spark)

    result shouldBe a[Right[_, _]]
    result.foreach { df =>
      df.schema("id").dataType shouldBe StringType
      df.count() shouldBe 2
    }
  }

  // ===========================================================================
  // 14. MultilineArray format — reads a valid JSON array file with correct row count
  // ===========================================================================

  it should "return Right(DataFrame) with the correct row count for a multiline JSON array" in {
    val content =
      """[
        |  {"id": 1, "name": "Alice", "amount": 10.5},
        |  {"id": 2, "name": "Bob",   "amount": 20.0},
        |  {"id": 3, "name": "Carol", "amount": 30.0}
        |]""".stripMargin

    val path = writeMultilineJson(content)
    val connector = new JsonFileConnector(
      schemaMode = JsonFileConnector.SchemaMode.Strict,
      jsonFormat = JsonFileConnector.JsonFormat.MultilineArray,
      schemaLoader = makeLoader(testSchema)
    )
    val config = makeConfig(filePath = Some(path))

    val result = connector.extract(config, spark)

    result shouldBe a[Right[_, _]]
    result.foreach(df => df.count() shouldBe 3)
  }

  // ===========================================================================
  // 15. MultilineArray format — schema is applied correctly to array elements
  // ===========================================================================

  it should "apply the registered schema to rows read from a multiline JSON array file" in {
    val content =
      """[
        |  {"id": 10, "name": "Dave", "amount": 99.5},
        |  {"id": 20, "name": "Eve",  "amount": 49.0}
        |]""".stripMargin

    val path = writeMultilineJson(content)
    val connector = new JsonFileConnector(
      schemaMode = JsonFileConnector.SchemaMode.Strict,
      jsonFormat = JsonFileConnector.JsonFormat.MultilineArray,
      schemaLoader = makeLoader(testSchema)
    )
    val config = makeConfig(filePath = Some(path))

    val result = connector.extract(config, spark)

    result shouldBe a[Right[_, _]]
    result.foreach { df =>
      df.schema.fieldNames.toSeq shouldBe Seq("id", "name", "amount")
      df.schema("id").dataType shouldBe LongType
    }
  }

  // ===========================================================================
  // 16. Flatten depth 0 — nested struct column preserved as StructType
  // ===========================================================================

  it should "preserve nested struct columns when flattenDepth is 0" in {
    val path = writeJsonLines(Seq(
      """{"id": 1, "event": {"type": "click", "ts": 1000}}""",
      """{"id": 2, "event": {"type": "view",  "ts": 2000}}"""
    ))
    val connector = new JsonFileConnector(
      schemaMode   = JsonFileConnector.SchemaMode.Strict,
      flattenDepth = 0,
      schemaLoader = makeLoader(nestedSchema)
    )
    val config = makeConfig(filePath = Some(path))

    val result = connector.extract(config, spark)

    result shouldBe a[Right[_, _]]
    result.foreach { df =>
      // 'event' column must remain a StructType — not expanded
      df.schema.fieldNames should contain("event")
      df.schema("event").dataType shouldBe a[StructType]
      df.count() shouldBe 2
    }
  }

  // ===========================================================================
  // 17. Flatten depth 1 — struct columns expanded to parent_child top-level columns
  // ===========================================================================

  it should "expand top-level StructType columns to parent_child columns when flattenDepth is 1" in {
    val path = writeJsonLines(Seq(
      """{"id": 1, "event": {"type": "click", "ts": 1000}}""",
      """{"id": 2, "event": {"type": "view",  "ts": 2000}}"""
    ))
    val connector = new JsonFileConnector(
      schemaMode   = JsonFileConnector.SchemaMode.Strict,
      flattenDepth = 1,
      schemaLoader = makeLoader(nestedSchema)
    )
    val config = makeConfig(filePath = Some(path))

    val result = connector.extract(config, spark)

    result shouldBe a[Right[_, _]]
    result.foreach { df =>
      // 'event' struct should be expanded to 'event_type' and 'event_ts'
      df.schema.fieldNames should contain("event_type")
      df.schema.fieldNames should contain("event_ts")
      // The original 'event' struct column should no longer exist
      df.schema.fieldNames should not contain "event"
      df.count() shouldBe 2
    }
  }

  // ===========================================================================
  // 18. Flatten depth 1 — non-struct columns pass through unchanged
  // ===========================================================================

  it should "leave non-struct top-level columns unchanged when flattenDepth is 1" in {
    val path = writeJsonLines(Seq(
      """{"id": 1, "event": {"type": "click", "ts": 1000}}""",
      """{"id": 2, "event": {"type": "view",  "ts": 2000}}"""
    ))
    val connector = new JsonFileConnector(
      schemaMode   = JsonFileConnector.SchemaMode.Strict,
      flattenDepth = 1,
      schemaLoader = makeLoader(nestedSchema)
    )
    val config = makeConfig(filePath = Some(path))

    val result = connector.extract(config, spark)

    result shouldBe a[Right[_, _]]
    result.foreach { df =>
      // 'id' is a plain LongType column — it should pass through untouched
      df.schema.fieldNames should contain("id")
      df.schema("id").dataType shouldBe LongType
    }
  }

  // ===========================================================================
  // 19. Flatten depth 1 — inner nested struct (depth 2) remains a StructType
  // ===========================================================================

  it should "not recurse into inner structs when flattenDepth is 1 (inner struct remains StructType)" in {
    val path = writeJsonLines(Seq(
      """{"id": 1, "outer": {"inner": {"val": "x"}}}""",
      """{"id": 2, "outer": {"inner": {"val": "y"}}}"""
    ))
    val connector = new JsonFileConnector(
      schemaMode   = JsonFileConnector.SchemaMode.Strict,
      flattenDepth = 1,
      schemaLoader = makeLoader(deeplyNestedSchema)
    )
    val config = makeConfig(filePath = Some(path))

    val result = connector.extract(config, spark)

    result shouldBe a[Right[_, _]]
    result.foreach { df =>
      // 'outer' is expanded to 'outer_inner'; 'outer_inner' is still a StructType
      df.schema.fieldNames should contain("outer_inner")
      df.schema("outer_inner").dataType shouldBe a[StructType]
      // 'outer' itself is gone
      df.schema.fieldNames should not contain "outer"
    }
  }

  // ===========================================================================
  // 20. Flatten depth > 1 — treated as 1 (same behaviour as depth 1)
  // ===========================================================================

  it should "treat flattenDepth > 1 identically to flattenDepth = 1" in {
    val path = writeJsonLines(Seq(
      """{"id": 1, "event": {"type": "click", "ts": 1000}}""",
      """{"id": 2, "event": {"type": "view",  "ts": 2000}}"""
    ))
    val connectorDepth2 = new JsonFileConnector(
      schemaMode   = JsonFileConnector.SchemaMode.Strict,
      flattenDepth = 5, // values > 1 treated as 1
      schemaLoader = makeLoader(nestedSchema)
    )
    val connectorDepth1 = new JsonFileConnector(
      schemaMode   = JsonFileConnector.SchemaMode.Strict,
      flattenDepth = 1,
      schemaLoader = makeLoader(nestedSchema)
    )
    val config = makeConfig(filePath = Some(path))

    val result2 = connectorDepth2.extract(config, spark)
    val result1 = connectorDepth1.extract(config, spark)

    result2 shouldBe a[Right[_, _]]
    result1 shouldBe a[Right[_, _]]

    // Both should produce the same top-level column names
    for {
      df2 <- result2
      df1 <- result1
    } {
      df2.schema.fieldNames.toSet shouldBe df1.schema.fieldNames.toSet
    }
  }

  // ===========================================================================
  // 21. CorruptRecord PERMISSIVE — bad JSON row has null typed fields
  // ===========================================================================

  it should "return a row with null typed fields for a corrupt JSON record in PERMISSIVE mode" in {
    val path = writeJsonLines(Seq(
      """{"id": 1, "name": "Alice", "amount": 10.5}""",
      """THIS IS NOT JSON""",
      """{"id": 2, "name": "Bob", "amount": 20.0}"""
    ))
    val connector = new JsonFileConnector(
      schemaMode        = JsonFileConnector.SchemaMode.Strict,
      corruptRecordMode = JsonFileConnector.CorruptRecordMode.Permissive,
      schemaLoader      = makeLoader(testSchema)
    )
    val config = makeConfig(filePath = Some(path))

    val result = connector.extract(config, spark)

    result shouldBe a[Right[_, _]]
    result.foreach { df =>
      // PERMISSIVE returns all rows — corrupt rows have null typed columns
      df.count() shouldBe 3
      df.filter(df("id").isNull).count() shouldBe 1L
    }
  }

  // ===========================================================================
  // 22. CorruptRecord DROPMALFORMED — bad row excluded, count correct
  // ===========================================================================

  it should "exclude corrupt JSON rows and return only valid rows in DROPMALFORMED mode" in {
    val path = writeJsonLines(Seq(
      """{"id": 1, "name": "Alice", "amount": 10.5}""",
      """NOT JSON""",
      """{"id": 2, "name": "Bob", "amount": 20.0}""",
      """ALSO NOT JSON""",
      """{"id": 3, "name": "Carol", "amount": 30.0}"""
    ))
    val connector = new JsonFileConnector(
      schemaMode        = JsonFileConnector.SchemaMode.Strict,
      corruptRecordMode = JsonFileConnector.CorruptRecordMode.DropMalformed,
      schemaLoader      = makeLoader(testSchema)
    )
    val config = makeConfig(filePath = Some(path))

    val result = connector.extract(config, spark)

    result shouldBe a[Right[_, _]]
    result.foreach { df =>
      df.count() shouldBe 3
      df.schema.fieldNames.toSeq shouldBe testSchema.fieldNames.toSeq
    }
  }
}
