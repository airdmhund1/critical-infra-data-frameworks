"""pytest suite for scripts.validate_config.

Tests call the ``validate(path)`` function directly (not via subprocess) to
keep the suite fast and deterministic.  Temporary invalid configs are written
to the ``tmp_path`` fixture directory provided by pytest.
"""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import pytest
import yaml

from scripts.validate_config import validate

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_EXAMPLES_DIR = _REPO_ROOT / "examples" / "configs"


def _write_yaml(tmp_path: Path, name: str, data: dict[str, Any]) -> str:
    """Serialise ``data`` to YAML and write it to ``tmp_path/<name>``.

    Args:
        tmp_path: pytest ``tmp_path`` fixture directory.
        name: Filename (without directory) for the temporary config.
        data: Python dict to serialise.

    Returns:
        Absolute path string to the written file.
    """
    file_path = tmp_path / name
    with file_path.open("w", encoding="utf-8") as fh:
        yaml.dump(data, fh, default_flow_style=False, allow_unicode=True)
    return str(file_path)


def _minimal_valid_config() -> dict[str, Any]:
    """Return a minimal but fully valid configuration dict.

    All ten required top-level sections are present with the minimum required
    fields.  Tests that exercise invalid configs copy this dict and mutate the
    relevant section so that only the field under test is wrong.

    Returns:
        A dict that passes ``validate()`` without errors.
    """
    return {
        "schemaVersion": "1.0",
        "metadata": {
            "sourceId": "test-source-001",
            "sourceName": "Test Source",
            "sector": "financial-services",
            "owner": "platform-team",
            "environment": "dev",
        },
        "connection": {
            "type": "file",
            "credentialsRef": "vault://secret/test/credentials",
            "filePath": "/data/test/",
            "fileFormat": "csv",
        },
        "ingestion": {
            "mode": "full",
        },
        "schemaEnforcement": {
            "enabled": False,
        },
        "qualityRules": {
            "enabled": False,
        },
        "quarantine": {
            "enabled": False,
        },
        "storage": {
            "layer": "bronze",
            "format": "delta",
            "path": "s3://datalake-bronze/test/",
        },
        "monitoring": {
            "metricsEnabled": False,
        },
        "audit": {
            "enabled": True,
        },
    }


# ---------------------------------------------------------------------------
# a. Valid examples pass
# ---------------------------------------------------------------------------


class TestValidExampleConfigs:
    """Confirm that the canonical example configs pass validation."""

    def test_financial_services_oracle_trades_is_valid(self) -> None:
        """financial-services-oracle-trades.yaml must exit 0."""
        path = str(_EXAMPLES_DIR / "financial-services-oracle-trades.yaml")
        is_valid, errors = validate(path)
        assert is_valid, f"Expected valid but got errors: {errors}"
        assert errors == []

    def test_energy_ev_telemetry_csv_is_valid(self) -> None:
        """energy-ev-telemetry-csv.yaml must exit 0."""
        path = str(_EXAMPLES_DIR / "energy-ev-telemetry-csv.yaml")
        is_valid, errors = validate(path)
        assert is_valid, f"Expected valid but got errors: {errors}"
        assert errors == []


# ---------------------------------------------------------------------------
# b. Missing required fields produce named-field errors
# ---------------------------------------------------------------------------


class TestMissingRequiredFields:
    """Missing required fields must produce error messages naming the field."""

    def test_missing_schema_version(self, tmp_path: Path) -> None:
        """Removing schemaVersion produces an error mentioning 'schemaVersion'."""
        config = _minimal_valid_config()
        del config["schemaVersion"]
        path = _write_yaml(tmp_path, "missing_schema_version.yaml", config)

        is_valid, errors = validate(path)

        assert not is_valid
        combined = " ".join(errors)
        assert "schemaVersion" in combined, (
            f"Expected 'schemaVersion' in errors but got: {errors}"
        )

    def test_missing_metadata_source_id(self, tmp_path: Path) -> None:
        """Removing metadata.sourceId produces an error mentioning 'sourceId'."""
        config = _minimal_valid_config()
        del config["metadata"]["sourceId"]
        path = _write_yaml(tmp_path, "missing_source_id.yaml", config)

        is_valid, errors = validate(path)

        assert not is_valid
        combined = " ".join(errors)
        assert "sourceId" in combined, (
            f"Expected 'sourceId' in errors but got: {errors}"
        )

    def test_missing_audit_section(self, tmp_path: Path) -> None:
        """Removing the entire audit section produces an error mentioning 'audit'."""
        config = _minimal_valid_config()
        del config["audit"]
        path = _write_yaml(tmp_path, "missing_audit.yaml", config)

        is_valid, errors = validate(path)

        assert not is_valid
        combined = " ".join(errors)
        assert "audit" in combined, (
            f"Expected 'audit' in errors but got: {errors}"
        )


# ---------------------------------------------------------------------------
# c. Invalid enum values produce clear errors
# ---------------------------------------------------------------------------


class TestInvalidEnumValues:
    """Invalid enum values must produce errors naming the field and value."""

    def test_connection_type_invalid_enum(self, tmp_path: Path) -> None:
        """connection.type: ftp must produce an error mentioning 'connection'."""
        config = _minimal_valid_config()
        config["connection"]["type"] = "ftp"
        path = _write_yaml(tmp_path, "invalid_conn_type.yaml", config)

        is_valid, errors = validate(path)

        assert not is_valid
        combined = " ".join(errors)
        assert "connection" in combined, (
            f"Expected 'connection' in errors but got: {errors}"
        )
        assert "ftp" in combined, (
            f"Expected invalid value 'ftp' in errors but got: {errors}"
        )

    def test_storage_layer_invalid_enum(self, tmp_path: Path) -> None:
        """storage.layer: platinum must produce an error mentioning 'storage' or 'layer'."""
        config = _minimal_valid_config()
        config["storage"]["layer"] = "platinum"
        path = _write_yaml(tmp_path, "invalid_storage_layer.yaml", config)

        is_valid, errors = validate(path)

        assert not is_valid
        combined = " ".join(errors)
        assert "storage" in combined or "layer" in combined, (
            f"Expected 'storage' or 'layer' in errors but got: {errors}"
        )

    def test_metadata_sector_invalid_enum(self, tmp_path: Path) -> None:
        """metadata.sector: manufacturing must produce an error mentioning 'sector'."""
        config = _minimal_valid_config()
        config["metadata"]["sector"] = "manufacturing"
        path = _write_yaml(tmp_path, "invalid_sector.yaml", config)

        is_valid, errors = validate(path)

        assert not is_valid
        combined = " ".join(errors)
        assert "sector" in combined, (
            f"Expected 'sector' in errors but got: {errors}"
        )


# ---------------------------------------------------------------------------
# d. Literal credentials in the connection block are flagged
# ---------------------------------------------------------------------------


class TestLiteralCredentialDetection:
    """Literal credentials in the connection block must cause validation to fail."""

    def test_connection_password_flagged(self, tmp_path: Path) -> None:
        """connection.password with a literal value must exit 1 naming 'password' or 'credentials'."""
        config = _minimal_valid_config()
        config["connection"]["password"] = "secret123"
        path = _write_yaml(tmp_path, "literal_password.yaml", config)

        is_valid, errors = validate(path)

        assert not is_valid
        combined = " ".join(errors).lower()
        assert "password" in combined or "credentials" in combined, (
            f"Expected 'password' or 'credentials' in errors but got: {errors}"
        )

    def test_connection_token_flagged(self, tmp_path: Path) -> None:
        """connection.token with a literal value must exit 1."""
        config = _minimal_valid_config()
        config["connection"]["token"] = "abc"
        path = _write_yaml(tmp_path, "literal_token.yaml", config)

        is_valid, errors = validate(path)

        assert not is_valid
        combined = " ".join(errors).lower()
        assert "token" in combined or "credentials" in combined, (
            f"Expected 'token' or 'credentials' in errors but got: {errors}"
        )

    def test_credentials_ref_is_not_false_positive(self, tmp_path: Path) -> None:
        """connection.credentialsRef pointing to Vault must exit 0 (not a false positive)."""
        config = _minimal_valid_config()
        # credentialsRef is already in the minimal config; confirm it passes.
        assert config["connection"]["credentialsRef"] == "vault://secret/test/credentials"
        path = _write_yaml(tmp_path, "valid_credentials_ref.yaml", config)

        is_valid, errors = validate(path)

        assert is_valid, (
            f"credentialsRef with a Vault path should be valid but got errors: {errors}"
        )


# ---------------------------------------------------------------------------
# e. File system error cases
# ---------------------------------------------------------------------------


class TestFileSystemErrors:
    """Validate graceful handling of file-not-found and YAML parse errors."""

    def test_nonexistent_file_returns_error(self) -> None:
        """A path that does not exist must return (False, [<message>])."""
        is_valid, errors = validate("/nonexistent/path/config.yaml")

        assert not is_valid
        assert len(errors) == 1
        assert "not found" in errors[0].lower() or "no such" in errors[0].lower()

    def test_invalid_yaml_returns_error(self, tmp_path: Path) -> None:
        """A file with invalid YAML must return (False, [<parse error message>])."""
        bad_yaml = tmp_path / "bad.yaml"
        bad_yaml.write_text(
            "schemaVersion: 1.0\n  invalid: indentation: here:\n",
            encoding="utf-8",
        )

        is_valid, errors = validate(str(bad_yaml))

        assert not is_valid
        assert len(errors) >= 1
        # Should mention parse/YAML in the error
        combined = " ".join(errors).lower()
        assert "yaml" in combined or "parse" in combined, (
            f"Expected YAML parse error message but got: {errors}"
        )
