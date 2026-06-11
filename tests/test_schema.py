"""
Author: L. Saetta
Date last modified: 2026-06-11
License: MIT
Description: Tests for Oracle checkpointer schema helpers.
"""

import pytest

from langgraph_oracle.errors import OracleADBConfigurationError
from langgraph_oracle.schema import OracleCheckpointTables, validate_identifier


def test_validate_identifier_returns_uppercase_identifier() -> None:
    """Verify that safe Oracle identifiers are normalized."""
    assert validate_identifier("lg_test") == "LG_TEST"


@pytest.mark.parametrize("identifier", ["1BAD", "BAD-NAME", "BAD NAME", "A" * 31])
def test_validate_identifier_rejects_unsafe_values(identifier: str) -> None:
    """Verify that unsafe identifiers cannot reach SQL interpolation."""
    with pytest.raises(OracleADBConfigurationError):
        validate_identifier(identifier)


def test_table_names_are_derived_from_prefix() -> None:
    """Verify generated table names for the default prefix."""
    tables = OracleCheckpointTables.from_prefix()

    assert tables.migrations == "LG_CHECKPOINT_MIGRATIONS"
    assert tables.checkpoints == "LG_CHECKPOINTS"
    assert tables.blobs == "LG_CHECKPOINT_BLOBS"
    assert tables.writes == "LG_CHECKPOINT_WRITES"
