package com.criticalinfra.engine

import com.criticalinfra.config.ConnectionType

// =============================================================================
// ConnectorRegistry — immutable registry mapping ConnectionType to SourceConnector
//
// The registry is constructed once at engine startup with all supported
// connector implementations. The engine calls lookup before each pipeline run
// to obtain the appropriate SourceConnector for the configured ConnectionType.
//
// Because the registry is immutable after construction, all lookups are
// thread-safe without synchronisation and the set of available connectors is
// fully determined by the caller that builds the engine, not by the engine
// itself. This makes it straightforward to substitute test doubles in unit
// tests and to extend the engine with new connector types without modifying
// engine code.
// =============================================================================

/** Immutable registry mapping [[com.criticalinfra.config.ConnectionType]] values to their
  * [[SourceConnector]] implementations.
  *
  * Connectors are registered at engine startup by supplying a complete `Map` to the constructor;
  * there is no mechanism to add or remove connectors after construction. This immutability guarantee
  * means all lookups are thread-safe without synchronisation and the set of available connectors is
  * fixed for the lifetime of the engine instance.
  *
  * Use [[ConnectorRegistry.apply]] to build a registry with connectors, or [[ConnectorRegistry.empty]]
  * to construct an empty registry in tests that need to exercise the `ConfigurationError` path.
  *
  * @param connectors
  *   Complete mapping of connection types to their connector implementations, supplied by the engine
  *   bootstrap code. An empty map is valid (see [[ConnectorRegistry.empty]]) but will always produce
  *   a `ConfigurationError` for any lookup.
  */
final class ConnectorRegistry(
    private val connectors: Map[ConnectionType, SourceConnector]
) {

  /** Looks up the [[SourceConnector]] registered for the given [[com.criticalinfra.config.ConnectionType]].
    *
    * @param connectionType
    *   The connection type whose connector implementation is required, as read from
    *   `connection.connectionType` in the pipeline configuration.
    * @return
    *   `Right(connector)` containing the registered [[SourceConnector]] if an implementation has
    *   been registered for `connectionType`, or
    *   `Left(ConfigurationError)` with `field = "connection.connectionType"` if no connector has
    *   been registered for the requested type. This indicates a bootstrap configuration error —
    *   either the engine was not initialised with the required connector, or the pipeline
    *   configuration references a connection type that this engine deployment does not support.
    */
  def lookup(connectionType: ConnectionType): Either[ConfigurationError, SourceConnector] =
    connectors.get(connectionType) match {
      case Some(connector) => Right(connector)
      case None =>
        Left(
          ConfigurationError(
            field = "connection.connectionType",
            message = s"No connector registered for source type: ${connectionType}"
          )
        )
    }
}

/** Factory for [[ConnectorRegistry]] instances.
  *
  * Provides a primary constructor wrapper for normal engine bootstrap and an `empty` factory for
  * test scenarios that need a registry guaranteed to return `ConfigurationError` on every lookup.
  */
object ConnectorRegistry {

  /** Constructs a [[ConnectorRegistry]] pre-loaded with the supplied connector implementations.
    *
    * This is the primary entry point used by engine bootstrap code. All connector types that the
    * engine is expected to handle in this deployment must be supplied here; any `ConnectionType`
    * absent from the map will cause `lookup` to return a `ConfigurationError` at pipeline execution
    * time.
    *
    * @param connectors
    *   Complete mapping of [[com.criticalinfra.config.ConnectionType]] values to their
    *   [[SourceConnector]] implementations. Must not be `null`; use an empty `Map` (or
    *   [[ConnectorRegistry.empty]]) if no connectors are available.
    * @return
    *   A new immutable [[ConnectorRegistry]] containing exactly the supplied entries.
    */
  def apply(connectors: Map[ConnectionType, SourceConnector]): ConnectorRegistry =
    new ConnectorRegistry(connectors)

  /** Constructs an empty [[ConnectorRegistry]] with no connectors registered.
    *
    * Provided for testing only. A registry built with this factory will always return
    * `Left(ConfigurationError)` for every call to `lookup`, regardless of the requested
    * [[com.criticalinfra.config.ConnectionType]]. It must not be used in production engine bootstrap
    * code.
    *
    * @return
    *   A new immutable [[ConnectorRegistry]] with an empty connector map.
    */
  def empty: ConnectorRegistry = new ConnectorRegistry(Map.empty)
}
