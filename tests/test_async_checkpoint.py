"""
Author: L. Saetta
Date last modified: 2026-06-11
License: MIT
Description: Tests for the async Oracle ADB LangGraph checkpointer.
"""

# pylint: disable=duplicate-code

from __future__ import annotations

import asyncio
from typing import Any
from unittest import mock

import oracledb
import pytest

from langgraph_oracle import AsyncOracleADBCheckpointer
from langgraph_oracle.errors import (
    OracleADBConfigurationError,
    OracleADBUnsupportedOperation,
)


class AsyncFakeCursor:
    """Small async cursor fake that records SQL calls."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []
        self.closed = False

    async def execute(self, statement: str, parameters: Any = None) -> None:
        """Record the executed statement."""
        self.calls.append((statement, parameters))

    async def fetchone(self) -> None:
        """Return no row by default."""
        return None

    async def fetchall(self) -> list[Any]:
        """Return no rows by default."""
        return []

    def close(self) -> None:
        """Record cursor closure."""
        self.closed = True


class AsyncFakeConnection:
    """Small async connection fake that records transaction boundaries."""

    def __init__(self) -> None:
        self.cursor_instance = AsyncFakeCursor()
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self) -> AsyncFakeCursor:
        """Return a reusable fake cursor."""
        return self.cursor_instance

    async def commit(self) -> None:
        """Record a commit."""
        self.commits += 1

    async def rollback(self) -> None:
        """Record a rollback."""
        self.rollbacks += 1

    async def close(self) -> None:
        """Record connection closure."""
        self.closed = True


class AsyncDuplicateCursor(AsyncFakeCursor):
    """Async cursor fake that raises ORA-00001 on execute."""

    async def execute(self, statement: str, parameters: Any = None) -> None:
        """Raise a mocked Oracle duplicate-key error."""
        self.calls.append((statement, parameters))
        error = mock.Mock()
        error.code = 1
        raise oracledb.DatabaseError(error)


class AsyncDuplicateConnection(AsyncFakeConnection):
    """Async connection fake that uses a duplicate-key cursor."""

    def __init__(self) -> None:
        super().__init__()
        self.cursor_instance = AsyncDuplicateCursor()


def test_public_import_exposes_async_checkpointer() -> None:
    """Verify the public package exports the async checkpointer."""
    assert AsyncOracleADBCheckpointer.__name__ == "AsyncOracleADBCheckpointer"


def test_async_checkpointer_requires_connection() -> None:
    """Verify that an async connection object is mandatory."""
    with pytest.raises(OracleADBConfigurationError):
        AsyncOracleADBCheckpointer(None)


def test_sync_methods_are_rejected_on_async_checkpointer() -> None:
    """Verify sync methods do not silently block on the async saver."""
    saver = AsyncOracleADBCheckpointer(AsyncFakeConnection())

    with pytest.raises(OracleADBUnsupportedOperation):
        saver.get_tuple({"configurable": {"thread_id": "thread-1"}})


def test_aput_stores_checkpoint_and_returns_next_config() -> None:
    """Verify aput emits Oracle writes and returns LangGraph next config."""
    connection = AsyncFakeConnection()
    saver = AsyncOracleADBCheckpointer(connection)
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

    next_config = asyncio.run(
        saver.aput(
            config,
            checkpoint,
            {"source": "input", "step": -1},
            {"primitive": "1", "complex": "1"},
        )
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


def test_aput_writes_uses_insert_only_merge_for_regular_writes() -> None:
    """Verify regular async writes do not update duplicate rows."""
    connection = AsyncFakeConnection()
    saver = AsyncOracleADBCheckpointer(connection)

    asyncio.run(
        saver.aput_writes(
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
    )

    statement = connection.cursor_instance.calls[0][0]
    assert "MERGE INTO LG_CHECKPOINT_WRITES" in statement
    assert "WHEN MATCHED THEN UPDATE" not in statement


def test_regular_duplicate_async_write_race_is_ignored() -> None:
    """Verify duplicate regular async writes are treated as idempotent."""
    connection = AsyncDuplicateConnection()
    saver = AsyncOracleADBCheckpointer(connection)

    asyncio.run(
        saver.aput_writes(
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
    )

    assert connection.commits == 1
    assert connection.rollbacks == 0
