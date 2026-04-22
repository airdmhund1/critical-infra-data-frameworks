package com.criticalinfra.config

// =============================================================================
// ConfigLoadError — sealed error hierarchy for the configuration loader
//
// All errors produced during config loading are represented as subtypes of
// this sealed trait, enabling exhaustive pattern matching at call sites and
// preventing silent failure modes that violate the project's quality-as-a-
// first-class-concern architecture principle.
// =============================================================================

/** Base type for all errors that can occur during configuration loading and validation.
  *
  * Callers receive an `Either[ConfigLoadError, SourceConfig]` from the loader. Pattern matching on
  * the left projection gives exhaustive, compile-checked error handling without any exception
  * leakage.
  */
sealed trait ConfigLoadError

/** Raised when a configuration field fails JSON Schema validation.
  *
  * This covers required-field absence, type mismatches, enum constraint violations, and
  * conditional-field rule failures (e.g., `incrementalColumn` missing when `mode` is
  * `incremental`).
  *
  * @param field
  *   Dot-separated path to the offending field (e.g., `"ingestion.incrementalColumn"`).
  * @param message
  *   Human-readable description of the validation failure.
  */
final case class SchemaValidationError(field: String, message: String) extends ConfigLoadError

/** Raised when the loader cannot resolve a secrets-manager reference at runtime.
  *
  * A resolution failure means the external secrets manager (HashiCorp Vault or AWS KMS) returned an
  * error or the reference path does not exist. The pipeline must not start if credentials cannot be
  * resolved.
  *
  * @param ref
  *   The `credentialsRef` value that could not be resolved (e.g.,
  *   `"vault://secret/data/oracle-tradedb-prod"`).
  * @param cause
  *   Human-readable description of why resolution failed.
  */
final case class SecretsResolutionError(ref: String, cause: String) extends ConfigLoadError

/** Raised when the raw YAML or HOCON input cannot be parsed into a configuration structure.
  *
  * This typically indicates malformed syntax (unclosed braces, invalid YAML indentation,
  * unsupported character encodings) before field-level validation begins.
  *
  * @param message
  *   Human-readable description of the parse failure, including position information where the
  *   underlying parser provides it.
  */
final case class ParseError(message: String) extends ConfigLoadError
