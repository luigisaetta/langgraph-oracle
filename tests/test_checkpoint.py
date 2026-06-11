"""
Author: L. Saetta
Date last modified: 2026-06-11
License: MIT
Description: Tests for the Oracle ADB LangGraph checkpointer.
"""

from __future__ import annotations

from typing import Any

import pytest

from langgraph_oracle import OracleADBCheckpointer
from langgraph_oracle.errors import (
    OracleADBConfigurationError,
    OracleADBUnsupportedOperation,
)


class FakeCursor:
    """Small cursor fake that records SQL calls."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []
        self.closed = False

    def execute(self, statement: str, parameters: Any = None) -> None:
        """Record the executed statement."""
        self.calls.append((statement, parameters))

    def executemany(self, statement: str, parameters: Any) -> None:
        """Record the executed statement."""
        self.calls.append((statement, parameters))

    def fetchone(self) -> None:
        """Return no row by default."""
        return None

    def fetchall(self) -> list[Any]:
        """Return no rows by default."""
        return []

    def close(self) -> None:
        """Record cursor closure."""
        self.closed = True


class FakeConnection:
    """Small connection fake that records transaction boundaries."""

    def __init__(self) -> None:
        self.cursor_instance = FakeCursor()
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self) -> FakeCursor:
        """Return a reusable fake cursor."""
        return self.cursor_instance

    def commit(self) -> None:
        """Record a commit."""
        self.commits += 1

    def rollback(self) -> None:
        """Record a rollback."""
        self.rollbacks += 1

    def close(self) -> None:
        """Record connection closure."""
        self.closed = True


def test_public_import_exposes_checkpointer() -> None:
    """Verify the public package exports the checkpointer."""
    assert OracleADBCheckpointer.__name__ == "OracleADBCheckpointer"


def test_checkpointer_requires_connection() -> None:
    """Verify that a connection object is mandatory."""
    with pytest.raises(OracleADBConfigurationError):
        OracleADBCheckpointer(None)


def test_put_stores_checkpoint_and_returns_next_config() -> None:
    """Verify put emits Oracle writes and returns LangGraph next config."""
    connection = FakeConnection()
    saver = OracleADBCheckpointer(connection)
    config = {"configurable": {"thread_id": "thread-1", "checkpoint_ns": ""}}
    checkpoint = {
        "v": 4,
        "id": "checkpoint-1",
        "ts": "2026-06-11T00:00:00+00:00",
        "channel_values": {"primitive": "ok", "complex": {"answer": 42}},
        "channel_versions": {"primitive": "1", "complex": "1"},
        "versions_seen": {},
        "updated_channels": None,
    }

    next_config = saver.put(
        config,
        checkpoint,
        {"source": "input", "step": -1},
        {"primitive": "1", "complex": "1"},
    )

    assert next_config == {
        "configurable": {
            "thread_id": "thread-1",
            "checkpoint_ns": "",
            "checkpoint_id": "checkpoint-1",
        }
    }
    assert connection.commits == 1
    assert len(connection.cursor_instance.calls) == 2
    assert "MERGE INTO LG_CHECKPOINT_BLOBS" in connection.cursor_instance.calls[0][0]
    assert "MERGE INTO LG_CHECKPOINTS" in connection.cursor_instance.calls[1][0]


def test_put_writes_requires_checkpoint_id() -> None:
    """Verify writes cannot be stored without a checkpoint id."""
    saver = OracleADBCheckpointer(FakeConnection())

    with pytest.raises(OracleADBConfigurationError):
        saver.put_writes(
            {"configurable": {"thread_id": "thread-1"}},
            [("messages", "hello")],
            task_id="task-1",
        )


def test_put_writes_uses_insert_only_merge_for_regular_writes() -> None:
    """Verify regular writes do not update duplicate rows."""
    connection = FakeConnection()
    saver = OracleADBCheckpointer(connection)

    saver.put_writes(
        {
            "configurable": {
                "thread_id": "thread-1",
                "checkpoint_ns": "",
                "checkpoint_id": "checkpoint-1",
            }
        },
        [("messages", "hello")],
        task_id="task-1",
    )

    statement = connection.cursor_instance.calls[0][0]
    assert "MERGE INTO LG_CHECKPOINT_WRITES" in statement
    assert "WHEN MATCHED THEN UPDATE" not in statement


def test_prune_keep_latest_is_explicitly_unsupported() -> None:
    """Verify unsafe DeltaChannel pruning is not silently performed."""
    saver = OracleADBCheckpointer(FakeConnection())

    with pytest.raises(OracleADBUnsupportedOperation):
        saver.prune(["thread-1"], strategy="keep_latest")
