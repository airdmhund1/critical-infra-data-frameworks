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
ThisBuild / coverageFailOnMinimum    := true

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
  semanticdbVersion := scalafixSemanticdb.revision,
  // Spark 3.5.x bundles jackson-module-scala 2.15.x, which requires
  // jackson-databind >= 2.15.0 and < 2.16.0.  The configLoader dependencies
  // (json-schema-validator, jackson-dataformat-yaml) pull in 2.17.x, which
  // breaks RDDOperationScope static initialisation during Spark tests.
  // Pin to 2.15.4 so both consumers are satisfied.
  dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.4",
  dependencyOverrides += "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % "2.15.4",
  dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-annotations" % "2.15.4",
  dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.15.4"
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
        Dependencies.deltaForTest ++
        Dependencies.config ++
        Dependencies.logging ++
        Dependencies.configLoader ++
        Dependencies.test,
    // Fork test JVM and pin it to Java 17 so that Spark's Hadoop dependency can call
    // Subject.getSubject() (permanently removed in Java 23+).  The add-opens flags
    // suppress module-encapsulation warnings on internal Hadoop/Netty reflection.
    // Run the forked JVM from the repo root so that relative paths used by
    // ConfigLoaderSpec (e.g. "examples/configs/...") resolve correctly.
    Test / fork            := true,
    Test / javaHome        := Some(file("/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home")),
    Test / baseDirectory   := (ThisBuild / baseDirectory).value,
    Test / javaOptions ++= Seq(
      "--add-opens=java.base/javax.security.auth=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    ),
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
