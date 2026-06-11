"""
Author: L. Saetta
Date last modified: 2026-06-11
License: MIT
Description: Project-specific exceptions for langgraph-oracle facilities.
"""


class LangGraphOracleError(Exception):
    """Base exception for all langgraph-oracle errors."""


class OracleADBConfigurationError(LangGraphOracleError):
    """Raised when checkpointer configuration is missing or invalid."""


class OracleADBDatabaseError(LangGraphOracleError):
    """Raised when an Oracle Database operation fails."""


class OracleADBUnsupportedOperation(LangGraphOracleError):
    """Raised when an operation is intentionally unsupported."""
