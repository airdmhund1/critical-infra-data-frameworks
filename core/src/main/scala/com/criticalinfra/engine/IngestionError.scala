package com.criticalinfra.engine

// =============================================================================
// IngestionError — sealed error hierarchy for the ingestion engine
//
// Every failure mode the engine can encounter is represented as a subtype of
// this sealed trait. Callers receive an Either[IngestionError, IngestionResult]
// from the engine, enabling exhaustive, compile-checked error handling without
// exception leakage.
//
// No exception is ever swallowed silently. Unexpected failures that do not fit
// a more specific subtype are wrapped in UnexpectedError so the original
// Throwable is preserved for diagnostics.
// =============================================================================

/** Base type for all errors that can occur during pipeline execution.
  *
  * Callers receive an `Either[IngestionError, IngestionResult]` from the engine. Exhaustive pattern
  * matching on the left projection guarantees that every failure mode is handled explicitly.
  */
sealed trait IngestionError

/** Raised when the source connector fails to read data from the external system.
  *
  * Covers JDBC connection failures, authentication errors, SQL execution errors, file-not-found
  * conditions, Kafka consumer errors, and any other transport-level or protocol-level failure
  * occurring during the extraction phase.
  *
  * @param source
  *   Identifier of the source system or connector that produced the failure (typically the
  *   `metadata.sourceId` from the pipeline configuration).
  * @param cause
  *   Human-readable description of the failure, including any error code or message supplied by the
  *   underlying connector.
  */
final case class ConnectorError(source: String, cause: String) extends IngestionError

/** Raised when the engine fails to write data to the Bronze lakehouse layer.
  *
  * Covers Delta Lake / Iceberg write failures, storage permission errors, path-not-found
  * conditions, serialisation errors, and any other failure occurring during the storage write
  * phase. The raw `path` is included so operators can inspect the target location directly.
  *
  * @param path
  *   URI of the Bronze storage path the engine attempted to write to.
  * @param cause
  *   Human-readable description of the write failure.
  */
final case class StorageWriteError(path: String, cause: String) extends IngestionError

/** Raised when the engine encounters an invalid or missing configuration value at runtime.
  *
  * Distinct from the config-loader's `SchemaValidationError` in that this error is produced by
  * the engine itself when it reads configuration values during pipeline execution — for example,
  * when it encounters an unrecognised `connectionType` string or a field required by the selected
  * connector is absent.
  *
  * @param field
  *   Name or dot-separated path of the configuration field that is invalid or missing.
  * @param message
  *   Human-readable description of why the field value is unacceptable.
  */
final case class ConfigurationError(field: String, message: String) extends IngestionError

/** Raised for any failure that does not map to a more specific [[IngestionError]] subtype.
  *
  * The optional `throwable` preserves the original exception for diagnostic logging and should
  * always be supplied when an exception is caught, so that stack trace information is not lost.
  *
  * @param message
  *   Human-readable description of the unexpected failure.
  * @param throwable
  *   The underlying exception, if available. Defaults to `None`.
  */
final case class UnexpectedError(
    message: String,
    throwable: Option[Throwable] = None
) extends IngestionError
