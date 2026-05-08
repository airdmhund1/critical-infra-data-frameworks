# =============================================================================
# Critical Infrastructure Data Frameworks — ingestion engine container
# =============================================================================
#
# Multi-stage Docker build for the Spark/Scala ingestion engine.
#
#   Stage 1  builder  — eclipse-temurin:11-jdk + sbt; produces the fat JAR.
#   Stage 2  runtime  — eclipse-temurin:11-jre; carries only the JAR + a
#                       non-root user. No JDK, no sbt, no source.
#
# Design goals:
#   * Reproducible, deterministic build (sbt cache layer keyed by build defs).
#   * Small runtime image — target < 600MB total (JRE base ~230MB + fat JAR
#     ~80-150MB + glibc + non-root user metadata).
#   * No credentials baked in — secrets are resolved at runtime from Vault /
#     AWS Secrets Manager (per project security model).
#   * Java 11 in BOTH stages (per Phase-1 acceptance criteria). The Java 17
#     `--add-opens` flags used in test JVMs are intentionally NOT applied
#     here; production runs target Java 11.
#   * Non-root runtime user (`cidf`) — defence-in-depth even though K8s
#     SecurityContext should also enforce runAsNonRoot.
#
# Build:
#   docker build -f deployment/docker/Dockerfile -t cidf-engine:0.1.0-SNAPSHOT .
#
# Run (engine entrypoint TBD — see issue tracker for Main class introduction):
#   docker run --rm cidf-engine:0.1.0-SNAPSHOT --help
#
# =============================================================================


# -----------------------------------------------------------------------------
# Stage 1 — builder
# -----------------------------------------------------------------------------
# Eclipse Temurin 11 JDK is the reference OpenJDK distribution. The `-jdk`
# variant is required here because sbt invokes javac during compilation.
# This stage is heavy (~500MB base + sbt + Ivy/Coursier cache); it is
# DISCARDED at the end of the build — only the produced JAR is copied
# into the runtime stage.
# -----------------------------------------------------------------------------
FROM eclipse-temurin:11-jdk AS builder

# sbt version pinned to match local development (project/build.properties
# pins this independently; we duplicate here so the apt-installed sbt
# launcher matches). The launcher self-bootstraps the correct sbt version
# from build.properties on first invocation, so a small drift is harmless.
ARG SBT_VERSION=1.10.1

# Install sbt via the official scala-sbt apt repository.
# `--no-install-recommends` keeps the layer slim. We clean apt lists in the
# same RUN to avoid persisting them in this layer.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl gnupg ca-certificates \
 && curl -fsSL "https://repo.scala-sbt.org/scalasbt/debian/sbt-${SBT_VERSION}.deb" -o /tmp/sbt.deb \
 && apt-get install -y --no-install-recommends /tmp/sbt.deb \
 && rm -f /tmp/sbt.deb \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# ---- Dependency-resolution layer ----
# Copy ONLY the build definition first. This makes `sbt update` cacheable:
# subsequent builds that touch only Scala source will reuse this layer
# instead of re-downloading the entire Ivy/Coursier cache.
COPY project/ project/
COPY build.sbt ./

# Pre-fetch all declared dependencies. Spark 3.5.x + Delta 3.x + AWS SDK v2
# is a large download (~700MB into the Coursier cache) — caching this layer
# is the single biggest build-time win.
RUN sbt update

# ---- Source layer ----
# Copy the four published submodules. Order is irrelevant here because all
# four arrive in a single layer; what matters is that this layer is INVALIDATED
# whenever any submodule source changes — the `sbt update` layer above is not.
COPY core/        core/
COPY connectors/  connectors/
COPY validation/  validation/
COPY monitoring/  monitoring/

# ---- Assembly ----
# `core/assembly` produces the fat JAR. `assemblyOption.withIncludeScala(false)`
# is set in build.sbt so the resulting JAR omits the Scala standard library
# (the deployment target is a Spark cluster which provides Scala on the
# classpath). Expected output:
#   /build/core/target/scala-2.13/critical-infra-core-assembly-0.1.0-SNAPSHOT.jar
RUN sbt core/assembly


# -----------------------------------------------------------------------------
# Stage 2 — runtime
# -----------------------------------------------------------------------------
# Eclipse Temurin 11 JRE — runtime-only, no compiler, no sbt. This is what
# keeps the final image under the 600MB target.
# -----------------------------------------------------------------------------
FROM eclipse-temurin:11-jre AS runtime

# OCI image labels — surfaces metadata in registry UIs (GHCR, Quay, ECR)
# and to security scanners. Kept inline (not at the top) because runtime-stage
# labels are what registries advertise.
LABEL org.opencontainers.image.source="https://github.com/airdmhund1/critical-infra-data-frameworks" \
      org.opencontainers.image.title="Critical Infrastructure Data Frameworks — Ingestion Engine" \
      org.opencontainers.image.description="Configuration-driven Spark/Scala ingestion engine for U.S. regulated-sector data systems (financial services, energy)." \
      org.opencontainers.image.version="0.1.0-SNAPSHOT" \
      org.opencontainers.image.licenses="Apache-2.0"

WORKDIR /app

# Create a dedicated non-root system user/group. Using a fixed UID/GID
# (10001) is friendlier to Kubernetes pod-security policies that enforce
# `runAsUser` ranges and to volume-mount permission planning.
#   --system           — system account, no aging, UID below 1000
#   --no-create-home   — runtime user has no $HOME requirement
#   --shell /sbin/nologin — defence in depth; no interactive login shell
RUN groupadd --system --gid 10001 cidf \
 && useradd  --system --uid 10001 --gid cidf \
             --no-create-home --shell /sbin/nologin cidf \
 && chown -R cidf:cidf /app

# Copy ONLY the produced fat JAR from the builder stage. The wildcard in the
# source path tolerates version-string drift; the destination is a stable name
# (`engine.jar`) so the entrypoint never has to encode a version.
COPY --from=builder --chown=cidf:cidf \
     /build/core/target/scala-2.13/critical-infra-core-assembly-*.jar \
     /app/engine.jar

# Drop privileges — every command from here on runs as `cidf`.
USER cidf

# Configuration is mounted at runtime (e.g. K8s ConfigMap → /etc/cidf, or
# `docker run -v $(pwd)/examples/configs:/etc/cidf:ro`). It is intentionally
# NOT baked into the image — keeps the image generic across deployments and
# avoids accidentally shipping environment-specific YAML.
#
# NOTE on entrypoint: the Main class will be wired in a later issue. For now
# `java -jar` runs whatever Main-Class the assembly manifest carries (none yet,
# so the container will exit with a "no main manifest attribute" error until
# that issue lands — this is intentional and documented).
ENTRYPOINT ["java", "-jar", "/app/engine.jar"]
