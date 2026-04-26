package com.criticalinfra.connectors.file

import com.criticalinfra.engine.SourceConnector

/** Marker trait for file-based source connectors.
  *
  * Extends [[SourceConnector]] to allow the connector registry and ingestion engine to distinguish
  * file-based connectors from JDBC and streaming connectors without requiring runtime type checks.
  */
trait FileSourceConnector extends SourceConnector
