# Contributing to Critical Infrastructure Data Frameworks

Thank you for your interest in contributing. This document covers everything you need to build,
test, and submit changes to this project.

## Building the Project

Prerequisites: Java 11 or 17 (Temurin recommended), sbt 1.9.x.

```bash
# Compile all modules
sbt compile

# Run all tests
sbt test

# Run a specific test class
sbt "testOnly *OracleJdbcConnectorSpec"

# Produce the fat JAR (core module)
sbt core/assembly

# Format all Scala sources
sbt scalafmtAll

# Check formatting without modifying files (used in CI)
sbt scalafmtCheck

# Run static analysis
sbt scalafix
```

## Branch Naming

Use the following prefixes, matching the Conventional Commits scope:

| Prefix      | When to use                                 |
|-------------|---------------------------------------------|
| `feat/`     | New feature or capability                   |
| `fix/`      | Bug fix                                     |
| `chore/`    | Build, tooling, or dependency changes       |
| `docs/`     | Documentation only                          |
| `test/`     | Test additions or corrections               |
| `refactor/` | Code restructuring without behaviour change |

Example: `feat/jdbc-oracle-connector`

## Commit Message Format

This project uses [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <short description>

[optional body]

[optional footer(s)]
```

Types: `feat`, `fix`, `chore`, `docs`, `test`, `refactor`, `perf`, `ci`

Scopes: `core`, `connectors`, `validation`, `monitoring`, `build`, `docs`

Examples:

```
feat(connectors): add watermark-based incremental extraction to JdbcConnector
fix(core): handle null partition values in BronzeLayerWriter
chore(build): upgrade Spark to 3.5.3
```

## Pull Request Process

1. Fork the repository and create a feature branch from `main`.
2. Ensure `sbt compile` succeeds with no warnings.
3. Ensure `sbt test` passes (all tests green).
4. Ensure `sbt scalafmtCheck` passes (no unformatted files).
5. Maintain or improve code coverage (minimum 80% statement coverage).
6. Open a PR against `main` with a clear description of:
   - What problem this solves
   - How it was tested
   - Any compliance or audit implications
7. CI must pass before review begins.
8. PRs are merged using squash-and-merge.

## Code Standards

- Functional Scala: prefer `Either`, `Try`, `Option`. Never swallow exceptions.
- No `var` unless justified with a comment explaining why mutation is necessary.
- All public APIs must have ScalaDoc.
- No credentials, secrets, or personal data in source files or configuration.
- No `@Ignore` annotations on tests in production code.

## Security

Never commit:
- `.env` files
- `*.credentials` or `*.credential` files
- `local.conf`
- Any file containing passwords, API keys, or certificates

All secrets are resolved at runtime from HashiCorp Vault or AWS KMS.

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.

## Questions

Open a GitHub Issue or contact the maintainer via the repository.
