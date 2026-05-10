"""Configuration validation CLI for the critical-infra-data-frameworks project.

Validates a YAML source-configuration file against the JSON Schema
(Draft 2019-09) defined in ``schemas/source-config-v1.json``.

Usage::

    python -m scripts.validate_config <path-to-yaml>

Exit codes:
    0 — valid
    1 — invalid (schema violations, file not found, YAML parse error, or
        literal credentials detected in the connection block)
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import jsonschema
import yaml

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SCHEMA_PATH = _REPO_ROOT / "schemas" / "source-config-v1.json"

# Keys that must never appear as literals inside the connection block.
# The JSON Schema ``not`` rule also covers this, but we add an explicit
# Python-level check so the error message names the offending key clearly.
_CREDENTIAL_KEYS = {"password", "secret", "token", "key", "credential"}


# ---------------------------------------------------------------------------
# Core validation logic
# ---------------------------------------------------------------------------


def _load_schema() -> dict[str, Any]:
    """Load the JSON Schema from the canonical path relative to the repo root.

    Returns:
        The parsed JSON Schema as a Python dict.

    Raises:
        FileNotFoundError: If the schema file does not exist at the expected
            location.
        json.JSONDecodeError: If the schema file cannot be parsed as JSON.
    """
    with _SCHEMA_PATH.open(encoding="utf-8") as fh:
        return json.load(fh)


def _check_literal_credentials(config: dict[str, Any]) -> list[str]:
    """Detect literal credentials inside the ``connection`` block.

    The JSON Schema ``not`` rule enforces this constraint at schema-validation
    time, but it produces a generic error message.  This explicit check runs
    first and returns a clear, field-named error for each offending key.

    Args:
        config: The parsed configuration dict.

    Returns:
        A list of human-readable error strings, one per offending key found.
        Empty if no literal credentials are present.
    """
    errors: list[str] = []
    connection = config.get("connection")
    if not isinstance(connection, dict):
        return errors

    for key, value in connection.items():
        if key.lower() in _CREDENTIAL_KEYS and value is not None:
            errors.append(
                f"connection.{key}: literal credentials detected — "
                f"use 'credentialsRef' pointing to Vault or AWS KMS instead "
                f"of embedding a {key!r} value directly in the config file"
            )
    return errors


def _format_validation_error(error: jsonschema.ValidationError) -> str:
    """Format a single jsonschema ``ValidationError`` into a field-named string.

    Args:
        error: A ``jsonschema.ValidationError`` instance from the validator.

    Returns:
        A string of the form ``<field-path>: <human-readable message>``.
    """
    # Build a dot-separated field path from the absolute_path deque.
    parts = list(error.absolute_path)
    if parts:
        field_path = ".".join(str(p) for p in parts)
    else:
        # Top-level error (e.g. missing required property) — extract the
        # property name from the validator_value when possible.
        if error.validator == "required" and isinstance(
            error.validator_value, list
        ):
            # The message contains the missing property name; use the path of
            # the containing object plus the property name for clarity.
            field_path = error.message.split("'")[1] if "'" in error.message else "(root)"
        else:
            field_path = "(root)"

    return f"{field_path}: {error.message}"


def validate(path: str) -> tuple[bool, list[str]]:
    """Validate a YAML configuration file against the source-config-v1 schema.

    This is the primary entry point for programmatic use and is called by the
    ``__main__`` block.  Tests import and call this function directly to avoid
    subprocess overhead.

    The function runs two independent checks in order:

    1. **Literal-credential check** — rejects any ``connection.*`` key whose
       name matches a known credential keyword (``password``, ``secret``,
       ``token``, ``key``, ``credential``), regardless of schema validity.
    2. **JSON Schema validation** — validates the full document against
       ``schemas/source-config-v1.json`` (Draft 2019-09).

    If either check fails, the function returns ``(False, errors)`` where
    ``errors`` is a non-empty list of human-readable strings each naming the
    exact field path that failed.

    Args:
        path: Absolute or relative path to the YAML configuration file.

    Returns:
        A ``(is_valid, error_messages)`` tuple.  ``is_valid`` is ``True`` only
        when every check passes.  ``error_messages`` is an empty list on
        success or a list of descriptive strings on failure.
    """
    config_path = Path(path)

    # --- file existence and YAML parse ---
    if not config_path.exists():
        return False, [f"File not found: {path}"]

    try:
        with config_path.open(encoding="utf-8") as fh:
            config = yaml.safe_load(fh)
    except yaml.YAMLError as exc:
        return False, [f"YAML parse error in {path}: {exc}"]

    if not isinstance(config, dict):
        return False, [f"Invalid config: expected a YAML mapping at the top level, got {type(config).__name__}"]

    errors: list[str] = []

    # --- explicit credential check (runs before schema validation) ---
    credential_errors = _check_literal_credentials(config)
    errors.extend(credential_errors)

    # --- JSON Schema validation ---
    try:
        schema = _load_schema()
    except FileNotFoundError:
        return False, [f"Schema file not found: {_SCHEMA_PATH}"]
    except json.JSONDecodeError as exc:
        return False, [f"Schema file is not valid JSON: {exc}"]

    validator_cls = jsonschema.Draft201909Validator
    validator = validator_cls(schema)

    schema_errors = sorted(
        validator.iter_errors(config),
        key=lambda e: list(e.absolute_path),
    )
    for error in schema_errors:
        formatted = _format_validation_error(error)
        # Suppress schema-level credential errors when our explicit check
        # already reported them — avoids duplicate messages.
        if credential_errors and "connection" in formatted and any(
            ck in formatted.lower() for ck in _CREDENTIAL_KEYS
        ):
            continue
        errors.append(formatted)

    is_valid = len(errors) == 0
    return is_valid, errors


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    """Build the argument parser for the validate_config CLI.

    Returns:
        A configured ``argparse.ArgumentParser`` instance.
    """
    parser = argparse.ArgumentParser(
        prog="python -m scripts.validate_config",
        description=(
            "Validate a YAML source-configuration file against the "
            "critical-infra-data-frameworks source-config-v1 JSON Schema. "
            "Exits 0 if valid, 1 if invalid."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python -m scripts.validate_config examples/configs/financial-services-oracle-trades.yaml\n"
            "  python -m scripts.validate_config my-source.yaml\n"
        ),
    )
    parser.add_argument(
        "config_path",
        metavar="CONFIG_PATH",
        help="Path to the YAML configuration file to validate.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Parse CLI arguments, run validation, and return an exit code.

    Args:
        argv: Optional argument list (defaults to ``sys.argv[1:]`` when
            ``None``).  Primarily useful for testing.

    Returns:
        ``0`` if the configuration is valid, ``1`` otherwise.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)

    is_valid, errors = validate(args.config_path)

    if is_valid:
        print("✓ valid")
        return 0
    else:
        for error in errors:
            print(error, file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
