"""
Author: L. Saetta
Date last modified: 2026-06-11
License: MIT
Description: Tests for the Oracle ADB LangGraph checkpointer.
"""

# pylint: disable=duplicate-code

from __future__ import annotations

from typing import Any
from unittest import mock

import oracledb
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


class FakePool:  # pylint: disable=too-few-public-methods
    """Small pool fake that returns one connection per acquire call."""

    def __init__(self) -> None:
        self.connections: list[FakeConnection] = []

    def acquire(self) -> FakeConnection:
        """Return a fresh fake connection."""
        connection = FakeConnection()
        self.connections.append(connection)
        return connection


class DuplicateCursor(FakeCursor):
    """Cursor fake that raises ORA-00001 on execute."""

    def execute(self, statement: str, parameters: Any = None) -> None:
        """Raise a mocked Oracle duplicate-key error."""
        self.calls.append((statement, parameters))
        error = mock.Mock()
        error.code = 1
        raise oracledb.DatabaseError(error)


class DuplicateConnection(FakeConnection):
    """Connection fake that uses a duplicate-key cursor."""

    def __init__(self) -> None:
        super().__init__()
        self.cursor_instance = DuplicateCursor()


class RunDeletingCursor(FakeCursor):
    """Cursor fake that returns one checkpoint key for run lookup."""

    def fetchall(self) -> list[Any]:
        """Return one run-owned checkpoint for lookup queries."""
        if self.calls and "JSON_VALUE" in self.calls[-1][0]:
            return [("thread-1", "", "checkpoint-1")]
        return []


class RunDeletingConnection(FakeConnection):
    """Connection fake for delete_for_runs tests."""

    def __init__(self) -> None:
        super().__init__()
        self.cursor_instance = RunDeletingCursor()


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


def test_regular_duplicate_write_race_is_ignored() -> None:
    """Verify duplicate regular writes are treated as idempotent."""
    connection = DuplicateConnection()
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

    assert connection.commits == 1
    assert connection.rollbacks == 0


def test_pool_acquires_connection_per_operation() -> None:
    """Verify pool-backed savers do not reuse a single connection."""
    pool = FakePool()
    saver = OracleADBCheckpointer(pool)

    saver.delete_thread("thread-1")
    saver.delete_thread("thread-2")

    assert len(pool.connections) == 2
    assert all(connection.closed for connection in pool.connections)


def test_thread_locks_are_returned_in_stable_order() -> None:
    """Verify multi-thread operations acquire locks in sorted order."""
    saver = OracleADBCheckpointer(FakeConnection())

    locks = saver._locks_for_threads(  # pylint: disable=protected-access
        ["thread-b", "thread-a"]
    )

    assert locks == [
        saver._thread_locks["thread-a"],  # pylint: disable=protected-access
        saver._thread_locks["thread-b"],  # pylint: disable=protected-access
    ]


def test_get_tuple_uses_thread_guard_for_reads() -> None:
    """Verify checkpoint tuple reads are guarded by thread id."""
    saver = OracleADBCheckpointer(FakeConnection())

    # pylint: disable=protected-access
    with mock.patch.object(saver, "_thread_guard", wraps=saver._thread_guard) as guard:
        saver.get_tuple({"configurable": {"thread_id": "thread-1"}})

    guard.assert_called_once_with("thread-1")


def test_delete_for_runs_guards_discovered_threads() -> None:
    """Verify run deletion serializes against discovered thread ids."""
    saver = OracleADBCheckpointer(RunDeletingConnection())

    # pylint: disable=protected-access
    with mock.patch.object(saver, "_thread_guard", wraps=saver._thread_guard) as guard:
        saver.delete_for_runs(["run-1"])

    guard.assert_called_once_with("thread-1")


def test_prune_keep_latest_is_explicitly_unsupported() -> None:
    """Verify unsafe DeltaChannel pruning is not silently performed."""
    saver = OracleADBCheckpointer(FakeConnection())

    with pytest.raises(OracleADBUnsupportedOperation):
        saver.prune(["thread-1"], strategy="keep_latest")
