// =============================================================================
// Critical Infrastructure Data Frameworks — root build definition
// =============================================================================

// ---------------------------------------------------------------------------
// ThisBuild settings — applied to every submodule
// ---------------------------------------------------------------------------

ThisBuild / organization := "com.criticalinfra"
ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := Dependencies.Versions.scala

// Compiler flags: enable additional linting
ThisBuild / scalacOptions ++= Seq(
  "-encoding",
  "UTF-8",
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Wunused:imports,privates,locals",
  "-language:higherKinds",
  "-language:implicitConversions"
)

// Minimum statement coverage across all modules
ThisBuild / coverageMinimumStmtTotal := 80
ThisBuild / coverageFailOnMinimum    := false // set true once coverage grows

// Delta Lake 3.x is published to Maven Central
ThisBuild / resolvers += Resolver.mavenCentral

// ---------------------------------------------------------------------------
// Assembly merge strategy (required for Spark fat JARs)
// ---------------------------------------------------------------------------

lazy val assemblyMergeStrategySettings = Seq(
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", "services", _*)            => MergeStrategy.concat
    case PathList("META-INF", "MANIFEST.MF")             => MergeStrategy.discard
    case PathList("META-INF", _*)                        => MergeStrategy.discard
    case PathList("reference.conf")                      => MergeStrategy.concat
    case PathList("application.conf")                    => MergeStrategy.concat
    case "git.properties"                                => MergeStrategy.discard
    case "log4j.properties"                              => MergeStrategy.discard
    case "log4j2.properties"                             => MergeStrategy.discard
    case PathList(ps @ _*) if ps.last.endsWith(".proto") => MergeStrategy.first
    case _                                               => MergeStrategy.first
  }
)

// ---------------------------------------------------------------------------
// Common settings shared by all submodules
// ---------------------------------------------------------------------------

lazy val commonSettings = Seq(
  // scalafix needs the semanticdb compiler plugin
  semanticdbEnabled := true,
  semanticdbVersion := scalafixSemanticdb.revision
)

// ---------------------------------------------------------------------------
// Root aggregate project
// ---------------------------------------------------------------------------

lazy val root = project
  .in(file("."))
  .aggregate(core, connectors, validation, monitoring)
  .settings(
    name           := "critical-infra-data-frameworks",
    publish / skip := true
  )

// ---------------------------------------------------------------------------
// core — Spark ingestion engine, config service, bronze-layer writer
// ---------------------------------------------------------------------------

lazy val core = project
  .in(file("core"))
  .settings(commonSettings)
  .settings(assemblyMergeStrategySettings)
  .settings(
    name := "core",
    libraryDependencies ++=
      Dependencies.spark ++
        Dependencies.delta ++
        Dependencies.config ++
        Dependencies.logging ++
        Dependencies.test,
    // Fat JAR: exclude Scala library — already present on the Spark cluster classpath
    assembly / assemblyOption :=
      (assembly / assemblyOption).value.withIncludeScala(false),
    assembly / assemblyJarName :=
      s"critical-infra-core-assembly-${version.value}.jar"
  )

// ---------------------------------------------------------------------------
// connectors — JDBC / file / streaming source connectors
// ---------------------------------------------------------------------------

lazy val connectors = project
  .in(file("connectors"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "connectors",
    libraryDependencies ++=
      Dependencies.spark ++
        Dependencies.delta ++
        Dependencies.config ++
        Dependencies.logging ++
        Dependencies.test
  )

// ---------------------------------------------------------------------------
// validation — data quality and quarantine engine (stub for Phase 1)
// ---------------------------------------------------------------------------

lazy val validation = project
  .in(file("validation"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "validation",
    libraryDependencies ++=
      Dependencies.spark ++
        Dependencies.config ++
        Dependencies.logging ++
        Dependencies.test
  )

// ---------------------------------------------------------------------------
// monitoring — Prometheus metrics emission (stub for Phase 1)
// ---------------------------------------------------------------------------

lazy val monitoring = project
  .in(file("monitoring"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "monitoring",
    libraryDependencies ++=
      Dependencies.config ++
        Dependencies.logging ++
        Dependencies.test
  )
