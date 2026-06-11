"""
Author: L. Saetta
Date last modified: 2026-06-11
License: MIT
Description: Oracle ADB schema names and migration SQL for LangGraph checkpoints.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from langgraph_oracle.errors import OracleADBConfigurationError

_VALID_IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,29}$")


def validate_identifier(identifier: str, *, label: str = "identifier") -> str:
    """Validate an Oracle SQL identifier used for generated table names.

    Args:
        identifier: Candidate identifier.
        label: Human-readable label used in error messages.

    Returns:
        The upper-case identifier.

    Raises:
        OracleADBConfigurationError: If the identifier is unsafe or too long.
    """
    if not _VALID_IDENTIFIER.fullmatch(identifier):
        raise OracleADBConfigurationError(
            f"Invalid Oracle {label}: use 1-30 letters, digits, or underscores, "
            "starting with a letter."
        )
    return identifier.upper()


@dataclass(frozen=True)
class OracleCheckpointTables:
    """Resolved table names for the Oracle ADB checkpointer schema."""

    prefix: str

    @classmethod
    def from_prefix(cls, prefix: str = "LG") -> "OracleCheckpointTables":
        """Build table names from a validated prefix.

        Args:
            prefix: Prefix for all checkpointer tables.

        Returns:
            Resolved table names.
        """
        return cls(prefix=validate_identifier(prefix, label="table prefix"))

    @property
    def migrations(self) -> str:
        """Return the migration tracking table name."""
        return f"{self.prefix}_CHECKPOINT_MIGRATIONS"

    @property
    def checkpoints(self) -> str:
        """Return the checkpoints table name."""
        return f"{self.prefix}_CHECKPOINTS"

    @property
    def blobs(self) -> str:
        """Return the checkpoint blobs table name."""
        return f"{self.prefix}_CHECKPOINT_BLOBS"

    @property
    def writes(self) -> str:
        """Return the checkpoint writes table name."""
        return f"{self.prefix}_CHECKPOINT_WRITES"

    @property
    def all_names(self) -> tuple[str, str, str, str]:
        """Return all table names in dependency order."""
        return (self.migrations, self.checkpoints, self.blobs, self.writes)


def create_table_statements(tables: OracleCheckpointTables) -> tuple[str, ...]:
    """Build CREATE TABLE statements for the initial schema.

    Args:
        tables: Resolved table names.

    Returns:
        SQL statements for the initial schema.
    """
    return (
        f"""
        CREATE TABLE {tables.migrations} (
            version_number NUMBER(10) PRIMARY KEY,
            applied_at TIMESTAMP WITH TIME ZONE
                DEFAULT SYSTIMESTAMP NOT NULL
        )
        """,
        f"""
        CREATE TABLE {tables.checkpoints} (
            thread_id VARCHAR2(512) NOT NULL,
            checkpoint_ns VARCHAR2(512) DEFAULT '' NOT NULL,
            checkpoint_id VARCHAR2(128) NOT NULL,
            parent_checkpoint_id VARCHAR2(128),
            checkpoint CLOB NOT NULL,
            metadata CLOB DEFAULT '{{}}' NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE
                DEFAULT SYSTIMESTAMP NOT NULL,
            CONSTRAINT {tables.prefix}_CKPT_PK
                PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id),
            CONSTRAINT {tables.prefix}_CKPT_JSON
                CHECK (checkpoint IS JSON),
            CONSTRAINT {tables.prefix}_CKPT_META_JSON
                CHECK (metadata IS JSON)
        )
        """,
        f"""
        CREATE TABLE {tables.blobs} (
            thread_id VARCHAR2(512) NOT NULL,
            checkpoint_ns VARCHAR2(512) DEFAULT '' NOT NULL,
            channel VARCHAR2(512) NOT NULL,
            version VARCHAR2(128) NOT NULL,
            type_tag VARCHAR2(256) NOT NULL,
            blob_value BLOB,
            CONSTRAINT {tables.prefix}_BLOB_PK
                PRIMARY KEY (thread_id, checkpoint_ns, channel, version)
        )
        """,
        f"""
        CREATE TABLE {tables.writes} (
            thread_id VARCHAR2(512) NOT NULL,
            checkpoint_ns VARCHAR2(512) DEFAULT '' NOT NULL,
            checkpoint_id VARCHAR2(128) NOT NULL,
            task_id VARCHAR2(512) NOT NULL,
            task_path VARCHAR2(1024) DEFAULT '' NOT NULL,
            write_idx NUMBER(10) NOT NULL,
            channel VARCHAR2(512) NOT NULL,
            type_tag VARCHAR2(256),
            blob_value BLOB NOT NULL,
            CONSTRAINT {tables.prefix}_WRITE_PK
                PRIMARY KEY (
                    thread_id,
                    checkpoint_ns,
                    checkpoint_id,
                    task_id,
                    write_idx
                )
        )
        """,
    )


def create_index_statements(tables: OracleCheckpointTables) -> tuple[str, ...]:
    """Build CREATE INDEX statements for the initial schema.

    Args:
        tables: Resolved table names.

    Returns:
        SQL statements for required indexes.
    """
    return (
        f"CREATE INDEX {tables.prefix}_CKPT_THREAD_IDX "
        f"ON {tables.checkpoints}(thread_id)",
        f"CREATE INDEX {tables.prefix}_BLOB_THREAD_IDX "
        f"ON {tables.blobs}(thread_id)",
        f"CREATE INDEX {tables.prefix}_WRITE_THREAD_IDX "
        f"ON {tables.writes}(thread_id)",
        f"CREATE INDEX {tables.prefix}_CKPT_LOOKUP_IDX "
        f"ON {tables.checkpoints}(thread_id, checkpoint_ns, checkpoint_id DESC)",
    )
