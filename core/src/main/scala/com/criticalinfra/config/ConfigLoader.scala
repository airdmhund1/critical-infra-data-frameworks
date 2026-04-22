package com.criticalinfra.config

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.networknt.schema.{JsonSchemaFactory, SpecVersion, ValidationMessage}
import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import org.slf4j.{Logger, LoggerFactory}

import java.io.{File, InputStream}
import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

// =============================================================================
// ConfigLoader — YAML/HOCON parsing, JSON Schema validation, and domain mapping
//
// Entry point: ConfigLoader.load(path, resolver)
//
// Pipeline:
//   1. Parse  — read file from local filesystem; produce a Jackson JsonNode
//   2. Validate — run networknt JSON Schema validation against source-config-v1.json
//   3. Map    — convert the validated JsonNode into a SourceConfig domain object
//
// S3 URI support is explicitly deferred (see parse step).
// Credentials are stored as-is; resolution is deferred to Branch 4.
// =============================================================================

/** Loads and validates a YAML or HOCON source configuration file, producing a fully mapped
  * [[SourceConfig]] or a structured [[ConfigLoadError]].
  *
  * The loader is stateless and purely functional: no mutable state, no thrown exceptions. All
  * failure modes are captured in the `Left` projection of the returned `Either`.
  *
  * Supported file formats:
  *   - `.yaml` / `.yml` — parsed with Jackson `ObjectMapper` + `YAMLFactory`
  *   - `.conf` / `.hocon` — parsed with Typesafe Config, then rendered to JSON so the same
  *     networknt schema validator can process it
  *
  * JSON Schema validation is performed against `schemas/source-config-v1.json` (loaded from the
  * classpath first; falls back to the working-directory path).
  */
object ConfigLoader {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private val yamlMapper: ObjectMapper = new ObjectMapper(new YAMLFactory())
  private val jsonMapper: ObjectMapper = new ObjectMapper()

  /** Path used for the filesystem fallback when the schema is not on the classpath. */
  private val SchemaClasspathResource: String = "schemas/source-config-v1.json"
  private val SchemaFilesystemPath: String    = "schemas/source-config-v1.json"

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  /** Loads a source configuration file, validates it against the JSON Schema, and maps it to a
    * [[SourceConfig]] domain object.
    *
    * The pipeline is: parse → validate → map → resolve credentials. Secrets resolution happens
    * last: once the YAML is validated the `credentialsRef` vault/KMS path is passed to `resolver`
    * and, if successful, replaced by the resolved plaintext value in the returned [[Connection]].
    * The resolved value is never written to any log.
    *
    * @param path
    *   Absolute or relative filesystem path to the configuration file. Files with a `.yaml` or
    *   `.yml` extension are parsed as YAML. Files with a `.conf` or `.hocon` extension are parsed
    *   as HOCON. S3 URIs (`s3://`, `s3a://`) are explicitly rejected — use a local path.
    * @param resolver
    *   Secrets resolver used to exchange the `credentialsRef` vault/KMS path for the actual secret
    *   value. If resolution fails the load returns `Left(SecretsResolutionError)`.
    * @return
    *   `Right(SourceConfig)` on success, or a `Left(ConfigLoadError)` describing the first error
    *   encountered in the parse → validate → map → resolve pipeline.
    */
  def load(path: String, resolver: SecretsResolver): Either[ConfigLoadError, SourceConfig] =
    for {
      node   <- parse(path)
      _      <- validate(node)
      config <- mapToSourceConfig(node, resolver)
    } yield config

  // ---------------------------------------------------------------------------
  // Step 1 — Parse
  // ---------------------------------------------------------------------------

  private def parse(path: String): Either[ConfigLoadError, JsonNode] = {
    val s3Prefix = path.startsWith("s3://") || path.startsWith("s3a://")
    if (s3Prefix) {
      Left(
        ParseError(
          "S3 URI loading not yet implemented — use a local path"
        )
      )
    }
    else {
      val file = new File(path)
      if (!file.exists()) {
        Left(ParseError(s"Config file not found: $path"))
      }
      else {
        val extension = path.toLowerCase.split("\\.").lastOption.getOrElse("")
        extension match {
          case "yaml" | "yml"   => parseYaml(file)
          case "conf" | "hocon" => parseHocon(file)
          case other =>
            Left(
              ParseError(
                s"Unsupported file extension '.$other'; expected .yaml, .yml, .conf, or .hocon"
              )
            )
        }
      }
    }
  }

  private def parseYaml(file: File): Either[ConfigLoadError, JsonNode] =
    Try(yamlMapper.readTree(file)) match {
      case Success(node) =>
        logger.debug("Parsed YAML config from {}", file.getAbsolutePath)
        Right(node)
      case Failure(cause) =>
        Left(ParseError(cause.getMessage))
    }

  private def parseHocon(file: File): Either[ConfigLoadError, JsonNode] =
    Try {
      val config     = ConfigFactory.parseFile(file).resolve()
      val jsonString = config.root().render(ConfigRenderOptions.concise())
      jsonMapper.readTree(jsonString)
    } match {
      case Success(node) =>
        logger.debug("Parsed HOCON config from {}", file.getAbsolutePath)
        Right(node)
      case Failure(cause) =>
        Left(ParseError(cause.getMessage))
    }

  // ---------------------------------------------------------------------------
  // Step 2 — Validate
  // ---------------------------------------------------------------------------

  private def validate(node: JsonNode): Either[ConfigLoadError, Unit] =
    loadSchema() match {
      case Left(err) => Left(err)
      case Right(schemaStream) =>
        Try {
          val factory    = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V201909)
          val schema     = factory.getSchema(schemaStream)
          val violations = schema.validate(node).asScala.toList
          violations
        } match {
          case Failure(cause) =>
            Left(ParseError(s"Schema validation engine error: ${cause.getMessage}"))
          case Success(Nil) =>
            logger.debug("JSON Schema validation passed")
            Right(())
          case Success(first :: _) =>
            // Report the first violation; field is the JSON pointer path
            val field   = extractField(first)
            val message = first.getMessage
            logger.debug("Schema validation failed at field '{}': {}", field, message)
            Left(SchemaValidationError(field, message))
        }
    }

  /** Load the schema as an InputStream — classpath first, filesystem fallback. */
  private def loadSchema(): Either[ConfigLoadError, InputStream] = {
    val classpathStream =
      Option(getClass.getClassLoader.getResourceAsStream(SchemaClasspathResource))

    classpathStream match {
      case Some(stream) =>
        logger.debug("Loaded JSON Schema from classpath: {}", SchemaClasspathResource)
        Right(stream)
      case None =>
        val fsPath = Paths.get(SchemaFilesystemPath)
        if (Files.exists(fsPath)) {
          logger.debug("Loaded JSON Schema from filesystem: {}", SchemaFilesystemPath)
          Try(Files.newInputStream(fsPath)) match {
            case Success(stream) => Right(stream)
            case Failure(cause) =>
              Left(
                ParseError(s"Cannot load JSON Schema: $SchemaFilesystemPath — ${cause.getMessage}")
              )
          }
        }
        else {
          Left(ParseError(s"Cannot load JSON Schema: $SchemaClasspathResource"))
        }
    }
  }

  /** Extract a human-readable dot-separated field path from a [[ValidationMessage]].
    *
    * networknt 1.4.x uses the `LEGACY` path type by default, which emits instance locations in
    * `$`-prefixed dot notation: `$` for the root object and `$.section.field` for nested paths. We
    * strip the `$.` prefix to produce a clean dot-path (e.g. `$.metadata.sector` becomes
    * `metadata.sector`). Root-level violations (`$` or an unexpectedly empty string) are normalised
    * to the sentinel `"(root)"`.
    *
    * [[ValidationMessage.getInstanceLocation]] is used (not `getEvaluationPath`) because it
    * identifies the location in the JSON *instance* that failed validation; `getEvaluationPath`
    * gives the path to the failing *schema* keyword instead.
    */
  private def extractField(msg: ValidationMessage): String = {
    val raw = msg.getInstanceLocation.toString
    raw match {
      case "$" | "" => "(root)"
      case other    => other.stripPrefix("$.").stripPrefix("$")
    }
  }

  // ---------------------------------------------------------------------------
  // Step 3 — Map JsonNode to SourceConfig
  // ---------------------------------------------------------------------------

  private def mapToSourceConfig(
      node: JsonNode,
      resolver: SecretsResolver
  ): Either[ConfigLoadError, SourceConfig] =
    Try {
      for {
        schemaVersion <- requireString(node, "schemaVersion")
        metadata      <- mapMetadata(node.get("metadata"))
        connection    <- mapConnection(node.get("connection"), resolver)
        ingestion     <- mapIngestion(node.get("ingestion"))
        schemaEnf     <- mapSchemaEnforcement(node.get("schemaEnforcement"))
        qualityRules  <- mapQualityRules(node.get("qualityRules"))
        quarantine    <- mapQuarantine(node.get("quarantine"))
        storage       <- mapStorage(node.get("storage"))
        monitoring    <- mapMonitoring(node.get("monitoring"))
        audit         <- mapAudit(node.get("audit"))
      } yield SourceConfig(
        schemaVersion = schemaVersion,
        metadata = metadata,
        connection = connection,
        ingestion = ingestion,
        schemaEnforcement = schemaEnf,
        qualityRules = qualityRules,
        quarantine = quarantine,
        storage = storage,
        monitoring = monitoring,
        audit = audit
      )
    } match {
      case Success(result) => result
      case Failure(cause)  => Left(ParseError(cause.getMessage))
    }

  // ---------------------------------------------------------------------------
  // Section mappers
  // ---------------------------------------------------------------------------

  private def mapMetadata(node: JsonNode): Either[ConfigLoadError, Metadata] =
    for {
      sourceId    <- requireString(node, "sourceId")
      sourceName  <- requireString(node, "sourceName")
      sectorStr   <- requireString(node, "sector")
      sector      <- parseSector(sectorStr)
      owner       <- requireString(node, "owner")
      envStr      <- requireString(node, "environment")
      environment <- parseEnvironment(envStr)
      tags = readStringList(node, "tags")
    } yield Metadata(
      sourceId = sourceId,
      sourceName = sourceName,
      sector = sector,
      owner = owner,
      environment = environment,
      tags = tags
    )

  private def mapConnection(
      node: JsonNode,
      resolver: SecretsResolver
  ): Either[ConfigLoadError, Connection] =
    for {
      typeStr        <- requireString(node, "type")
      connectionType <- parseConnectionType(typeStr)
      rawRef         <- requireString(node, "credentialsRef")
      // Resolve the vault/KMS reference to the actual secret value at load time.
      // The resolved value is stored in credentialsRef and must never be logged.
      credentialsRef <- resolver.resolve(rawRef)
      host          = optString(node, "host")
      port          = optInt(node, "port")
      database      = optString(node, "database")
      jdbcDriverStr = optString(node, "jdbcDriver")
      jdbcDriver <- jdbcDriverStr match {
        case Some(v) => parseJdbcDriver(v).map(Some(_))
        case None    => Right(None)
      }
      filePath      = optString(node, "filePath")
      fileFormatStr = optString(node, "fileFormat")
      fileFormat <- fileFormatStr match {
        case Some(v) => parseFileFormat(v).map(Some(_))
        case None    => Right(None)
      }
      kafkaTopic            = optString(node, "kafkaTopic")
      kafkaBootstrapServers = optString(node, "kafkaBootstrapServers")
    } yield Connection(
      connectionType = connectionType,
      credentialsRef = credentialsRef,
      host = host,
      port = port,
      database = database,
      jdbcDriver = jdbcDriver,
      filePath = filePath,
      fileFormat = fileFormat,
      kafkaTopic = kafkaTopic,
      kafkaBootstrapServers = kafkaBootstrapServers
    )

  private def mapIngestion(node: JsonNode): Either[ConfigLoadError, Ingestion] =
    for {
      modeStr <- requireString(node, "mode")
      mode    <- parseIngestionMode(modeStr)
      incrementalCol   = optString(node, "incrementalColumn")
      watermarkStorage = optString(node, "watermarkStorage")
      batchSize        = optInt(node, "batchSize").getOrElse(10000)
      parallelism      = optInt(node, "parallelism").getOrElse(4)
      schedule         = optString(node, "schedule")
      timeout          = optInt(node, "timeout").getOrElse(3600)
    } yield Ingestion(
      mode = mode,
      incrementalColumn = incrementalCol,
      watermarkStorage = watermarkStorage,
      batchSize = batchSize,
      parallelism = parallelism,
      schedule = schedule,
      timeout = timeout
    )

  private def mapSchemaEnforcement(node: JsonNode): Either[ConfigLoadError, SchemaEnforcement] =
    for {
      enabled <- requireBoolean(node, "enabled")
      modeStr = optString(node, "mode")
      enfMode <- modeStr match {
        case Some(v) => parseSchemaEnforcementMode(v).map(Some(_))
        case None    => Right(None)
      }
      registryRef = optString(node, "registryRef")
    } yield SchemaEnforcement(
      enabled = enabled,
      mode = enfMode,
      registryRef = registryRef
    )

  private def mapQualityRules(node: JsonNode): Either[ConfigLoadError, QualityRules] =
    for {
      enabled <- requireBoolean(node, "enabled")
    } yield QualityRules(enabled = enabled)

  private def mapQuarantine(node: JsonNode): Either[ConfigLoadError, Quarantine] =
    for {
      enabled <- requireBoolean(node, "enabled")
      path                = optString(node, "path")
      retentionDays       = optInt(node, "retentionDays").getOrElse(90)
      errorClassification = optBoolean(node, "errorClassification").getOrElse(true)
    } yield Quarantine(
      enabled = enabled,
      path = path,
      retentionDays = retentionDays,
      errorClassification = errorClassification
    )

  private def mapStorage(node: JsonNode): Either[ConfigLoadError, Storage] =
    for {
      layerStr  <- requireString(node, "layer")
      layer     <- parseStorageLayer(layerStr)
      formatStr <- requireString(node, "format")
      format    <- parseStorageFormat(formatStr)
      path      <- requireString(node, "path")
      partitionBy       = readStringList(node, "partitionBy")
      compactionEnabled = optBoolean(node, "compactionEnabled").getOrElse(false)
    } yield Storage(
      layer = layer,
      format = format,
      path = path,
      partitionBy = partitionBy,
      compactionEnabled = compactionEnabled
    )

  private def mapMonitoring(node: JsonNode): Either[ConfigLoadError, Monitoring] =
    for {
      metricsEnabled <- requireBoolean(node, "metricsEnabled")
      prometheusPort      = optInt(node, "prometheusPort").getOrElse(9090)
      slaThresholdSeconds = optInt(node, "slaThresholdSeconds")
      alertOnFailure      = optBoolean(node, "alertOnFailure").getOrElse(true)
    } yield Monitoring(
      metricsEnabled = metricsEnabled,
      prometheusPort = prometheusPort,
      slaThresholdSeconds = slaThresholdSeconds,
      alertOnFailure = alertOnFailure
    )

  private def mapAudit(node: JsonNode): Either[ConfigLoadError, Audit] =
    for {
      enabled <- requireBoolean(node, "enabled")
      lineageTracking     = optBoolean(node, "lineageTracking").getOrElse(true)
      immutableRawEnabled = optBoolean(node, "immutableRawEnabled").getOrElse(true)
      retentionDays       = optInt(node, "retentionDays").getOrElse(2555)
    } yield Audit(
      enabled = enabled,
      lineageTracking = lineageTracking,
      immutableRawEnabled = immutableRawEnabled,
      retentionDays = retentionDays
    )

  // ---------------------------------------------------------------------------
  // Enum parsers — return Left(SchemaValidationError) on unrecognised values
  // ---------------------------------------------------------------------------

  private def parseSector(value: String): Either[ConfigLoadError, Sector] =
    value match {
      case "financial-services" => Right(Sector.FinancialServices)
      case "energy"             => Right(Sector.Energy)
      case "healthcare"         => Right(Sector.Healthcare)
      case "government"         => Right(Sector.Government)
      case other => Left(SchemaValidationError("metadata.sector", s"Unknown value: $other"))
    }

  private def parseEnvironment(value: String): Either[ConfigLoadError, Environment] =
    value match {
      case "dev"     => Right(Environment.Dev)
      case "staging" => Right(Environment.Staging)
      case "prod"    => Right(Environment.Prod)
      case other => Left(SchemaValidationError("metadata.environment", s"Unknown value: $other"))
    }

  private def parseConnectionType(value: String): Either[ConfigLoadError, ConnectionType] =
    value match {
      case "jdbc"  => Right(ConnectionType.Jdbc)
      case "file"  => Right(ConnectionType.File)
      case "kafka" => Right(ConnectionType.Kafka)
      case "api"   => Right(ConnectionType.Api)
      case other   => Left(SchemaValidationError("connection.type", s"Unknown value: $other"))
    }

  private def parseJdbcDriver(value: String): Either[ConfigLoadError, JdbcDriver] =
    value match {
      case "oracle"   => Right(JdbcDriver.Oracle)
      case "postgres" => Right(JdbcDriver.Postgres)
      case "teradata" => Right(JdbcDriver.Teradata)
      case other => Left(SchemaValidationError("connection.jdbcDriver", s"Unknown value: $other"))
    }

  private def parseFileFormat(value: String): Either[ConfigLoadError, FileFormat] =
    value match {
      case "csv"     => Right(FileFormat.Csv)
      case "parquet" => Right(FileFormat.Parquet)
      case "json"    => Right(FileFormat.Json)
      case "avro"    => Right(FileFormat.Avro)
      case other => Left(SchemaValidationError("connection.fileFormat", s"Unknown value: $other"))
    }

  private def parseIngestionMode(value: String): Either[ConfigLoadError, IngestionMode] =
    value match {
      case "full"        => Right(IngestionMode.Full)
      case "incremental" => Right(IngestionMode.Incremental)
      case other         => Left(SchemaValidationError("ingestion.mode", s"Unknown value: $other"))
    }

  private def parseSchemaEnforcementMode(
      value: String
  ): Either[ConfigLoadError, SchemaEnforcementMode] =
    value match {
      case "strict"     => Right(SchemaEnforcementMode.Strict)
      case "permissive" => Right(SchemaEnforcementMode.Permissive)
      case other => Left(SchemaValidationError("schemaEnforcement.mode", s"Unknown value: $other"))
    }

  private def parseStorageLayer(value: String): Either[ConfigLoadError, StorageLayer] =
    value match {
      case "bronze" => Right(StorageLayer.Bronze)
      case "silver" => Right(StorageLayer.Silver)
      case "gold"   => Right(StorageLayer.Gold)
      case other    => Left(SchemaValidationError("storage.layer", s"Unknown value: $other"))
    }

  private def parseStorageFormat(value: String): Either[ConfigLoadError, StorageFormat] =
    value match {
      case "delta"   => Right(StorageFormat.Delta)
      case "iceberg" => Right(StorageFormat.Iceberg)
      case other     => Left(SchemaValidationError("storage.format", s"Unknown value: $other"))
    }

  // ---------------------------------------------------------------------------
  // JsonNode field accessors — each returns Left on missing required fields
  // ---------------------------------------------------------------------------

  private def requireString(node: JsonNode, field: String): Either[ConfigLoadError, String] =
    Option(node).flatMap(n => Option(n.get(field))) match {
      case Some(n) if n.isTextual => Right(n.asText())
      case Some(_) => Left(SchemaValidationError(field, s"Field '$field' must be a string"))
      case None    => Left(SchemaValidationError(field, s"Required field '$field' is missing"))
    }

  private def requireBoolean(node: JsonNode, field: String): Either[ConfigLoadError, Boolean] =
    Option(node).flatMap(n => Option(n.get(field))) match {
      case Some(n) if n.isBoolean => Right(n.asBoolean())
      case Some(_) => Left(SchemaValidationError(field, s"Field '$field' must be a boolean"))
      case None    => Left(SchemaValidationError(field, s"Required field '$field' is missing"))
    }

  private def optString(node: JsonNode, field: String): Option[String] =
    Option(node).flatMap(n => Option(n.get(field))).filter(_.isTextual).map(_.asText())

  private def optInt(node: JsonNode, field: String): Option[Int] =
    Option(node).flatMap(n => Option(n.get(field))).filter(_.isNumber).map(_.asInt())

  private def optBoolean(node: JsonNode, field: String): Option[Boolean] =
    Option(node).flatMap(n => Option(n.get(field))).filter(_.isBoolean).map(_.asBoolean())

  private def readStringList(node: JsonNode, field: String): List[String] =
    Option(node).flatMap(n => Option(n.get(field))) match {
      case Some(arr) if arr.isArray =>
        arr.elements().asScala.filter(_.isTextual).map(_.asText()).toList
      case _ => List.empty
    }
}
