"""Shared pytest fixtures for the validate_config test suite."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

# Resolve the repo root relative to this conftest file so that fixture paths
# work regardless of the working directory from which pytest is invoked.
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_EXAMPLES_DIR = _REPO_ROOT / "examples" / "configs"


@pytest.fixture()
def valid_financial_config() -> dict[str, Any]:
    """Return the parsed dict for the financial-services-oracle-trades example.

    Returns:
        The parsed YAML content of
        ``examples/configs/financial-services-oracle-trades.yaml``.
    """
    config_path = _EXAMPLES_DIR / "financial-services-oracle-trades.yaml"
    with config_path.open(encoding="utf-8") as fh:
        return yaml.safe_load(fh)


@pytest.fixture()
def valid_energy_config() -> dict[str, Any]:
    """Return the parsed dict for the energy-ev-telemetry-csv example.

    Returns:
        The parsed YAML content of
        ``examples/configs/energy-ev-telemetry-csv.yaml``.
    """
    config_path = _EXAMPLES_DIR / "energy-ev-telemetry-csv.yaml"
    with config_path.open(encoding="utf-8") as fh:
        return yaml.safe_load(fh)
