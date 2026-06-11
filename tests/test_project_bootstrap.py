"""
Author: L. Saetta
Date last modified: 2026-06-11
License: MIT
Description: Tests for the initial project and pytest configuration.
"""

from pathlib import Path

import langgraph_oracle


def test_package_exposes_version() -> None:
    """Verify that the package can be imported by pytest."""
    assert langgraph_oracle.__version__ == "0.1.0"


def test_checkpointer_spec_exists() -> None:
    """Verify that the first feature has a specification before implementation."""
    spec_path = Path("specs/oracle_adb_checkpointer.md")
    assert spec_path.exists()
    assert "Oracle ADB LangGraph Checkpointer Specification" in spec_path.read_text(
        encoding="utf-8"
    )
