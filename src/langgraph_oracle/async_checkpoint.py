"""
Author: L. Saetta
Date last modified: 2026-06-11
License: MIT
Description: Async LangGraph checkpointer implementation backed by Oracle ADB.
"""

# pylint: disable=duplicate-code

from __future__ import annotations

import asyncio
import json
import random
from collections.abc import AsyncIterator, Iterator, Sequence
from contextlib import asynccontextmanager, nullcontext
from typing import Any, Protocol, cast

import oracledb
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    WRITES_IDX_MAP,
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    get_checkpoint_id,
    get_serializable_checkpoint_metadata,
)
from langgraph.checkpoint.serde.base import SerializerProtocol

from langgraph_oracle.errors import (
    OracleADBConfigurationError,
    OracleADBDatabaseError,
    OracleADBUnsupportedOperation,
)
from langgraph_oracle.schema import (
    OracleCheckpointTables,
    create_index_statements,
    create_table_statements,
)
from langgraph_oracle.serialization import (
    dump_blobs,
    dump_writes,
    load_blobs,
    load_writes,
    split_checkpoint_blobs,
)
from langgraph_oracle.sql import (
    checkpoint_by_id_sql,
    checkpoint_keys_for_run_sql,
    latest_checkpoint_sql,
    list_checkpoints_query,
    merge_blob_sql,
    merge_checkpoint_sql,
    merge_write_sql,
    select_blob_sql,
    select_writes_sql,
    setup_migration_sql,
)


class _AsyncCursor(Protocol):
    """Protocol for the async cursor surface used by the checkpointer."""

    async def execute(self, statement: str, parameters: Any = None) -> Any:
        """Execute one SQL statement."""

    async def fetchone(self) -> Any:
        """Fetch one row."""

    async def fetchall(self) -> list[Any]:
        """Fetch all rows."""

    def close(self) -> None:
        """Close the cursor."""


class AsyncOracleADBCheckpointer(BaseCheckpointSaver[str]):
    """Async LangGraph checkpointer backed by Oracle Autonomous Database.

    The implementation uses native `oracledb` async connections and pools. It
    intentionally does not wrap synchronous database calls.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        conn: Any,
        *,
        table_prefix: str = "LG",
        serde: SerializerProtocol | None = None,
        commit_on_success: bool = True,
        close_on_exit: bool = False,
    ) -> None:
        """Initialize the async Oracle ADB checkpointer.

        Args:
            conn: Existing `oracledb.AsyncConnection` or
                `oracledb.AsyncConnectionPool`.
            table_prefix: Prefix for checkpointer tables.
            serde: Optional LangGraph serializer.
            commit_on_success: Commit write transactions after successful
                operations.
            close_on_exit: Close the connection or pool when the saver exits its
                async context manager.

        Raises:
            OracleADBConfigurationError: If the connection object is missing.
        """
        if conn is None:
            raise OracleADBConfigurationError(
                "An oracledb async connection is required."
            )

        super().__init__(serde=serde)
        self.conn = conn
        self.tables = OracleCheckpointTables.from_prefix(table_prefix)
        self.commit_on_success = commit_on_success
        self.close_on_exit = close_on_exit
        self._connection_lock = asyncio.Lock()
        self._thread_locks: dict[str, asyncio.Lock] = {}
        self._thread_locks_guard = asyncio.Lock()

    @classmethod
    @asynccontextmanager
    async def from_connection_params(
        cls,
        *,
        table_prefix: str = "LG",
        serde: SerializerProtocol | None = None,
        commit_on_success: bool = True,
        **connect_kwargs: Any,
    ) -> AsyncIterator["AsyncOracleADBCheckpointer"]:
        """Create a checkpointer that owns an async `oracledb` connection.

        Args:
            table_prefix: Prefix for checkpointer tables.
            serde: Optional LangGraph serializer.
            commit_on_success: Commit write transactions after successful
                operations.
            **connect_kwargs: Arguments passed to `oracledb.connect_async`.

        Yields:
            An async Oracle ADB checkpointer.
        """
        connection = await oracledb.connect_async(**connect_kwargs)
        saver = cls(
            connection,
            table_prefix=table_prefix,
            serde=serde,
            commit_on_success=commit_on_success,
            close_on_exit=True,
        )
        try:
            yield saver
        finally:
            await saver.aclose()

    async def __aenter__(self) -> "AsyncOracleADBCheckpointer":
        """Return this saver when used as an async context manager."""
        return self

    async def __aexit__(self, *_exc_info: object) -> None:
        """Close the underlying async resource when configured to do so."""
        await self.aclose()

    async def aclose(self) -> None:
        """Close the async connection or pool if this saver owns it."""
        if self.close_on_exit and hasattr(self.conn, "close"):
            await self.conn.close()

    async def setup(self) -> None:
        """Create or migrate the Oracle checkpoint schema asynchronously."""
        async with self._managed_cursor(write=True) as cursor:
            for statement in create_table_statements(self.tables):
                await self._execute_ddl_if_missing(cursor, statement)
            for statement in create_index_statements(self.tables):
                await self._execute_ddl_if_missing(cursor, statement)
            await cursor.execute(setup_migration_sql(self.tables), (0,))

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        """Fetch a checkpoint tuple using LangGraph async configuration."""
        thread_id, checkpoint_ns, checkpoint_id = self._config_values(config)
        if checkpoint_id is None:
            statement = latest_checkpoint_sql(self.tables)
            parameters = (thread_id, checkpoint_ns)
        else:
            statement = checkpoint_by_id_sql(self.tables)
            parameters = (thread_id, checkpoint_ns, checkpoint_id)

        async with self._thread_guard(thread_id):
            async with self._managed_cursor(write=False) as cursor:
                await cursor.execute(statement, parameters)
                row = await cursor.fetchone()
                if row is None:
                    return None
                return await self._load_checkpoint_tuple(cursor, row)

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,  # pylint: disable=redefined-builtin
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[CheckpointTuple]:
        """List checkpoints matching LangGraph filters asynchronously."""
        statement, parameters = self._list_query(config=config, before=before)
        yielded = 0

        async with self._read_thread_guard(config):
            async with self._managed_cursor(write=False) as cursor:
                await cursor.execute(statement, parameters)
                for row in await cursor.fetchall():
                    checkpoint_tuple = await self._load_checkpoint_tuple(cursor, row)
                    if filter and not self._metadata_matches(
                        checkpoint_tuple.metadata, filter
                    ):
                        continue
                    yield checkpoint_tuple
                    yielded += 1
                    if limit is not None and yielded >= limit:
                        break

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """Store a LangGraph checkpoint asynchronously."""
        thread_id, checkpoint_ns, parent_checkpoint_id = self._config_values(config)
        checkpoint_copy, blob_values = split_checkpoint_blobs(checkpoint)
        blob_rows = dump_blobs(
            serde=self.serde,
            thread_id=thread_id,
            checkpoint_ns=checkpoint_ns,
            values=blob_values,
            versions=new_versions,
        )
        checkpoint_metadata = get_serializable_checkpoint_metadata(config, metadata)

        async with self._thread_guard(thread_id):
            async with self._managed_cursor(write=True) as cursor:
                for row in blob_rows:
                    await cursor.execute(merge_blob_sql(self.tables), row)
                await cursor.execute(
                    merge_checkpoint_sql(self.tables),
                    (
                        thread_id,
                        checkpoint_ns,
                        checkpoint["id"],
                        parent_checkpoint_id,
                        self._to_json(checkpoint_copy),
                        self._to_json(checkpoint_metadata),
                    ),
                )

        return {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint["id"],
            }
        }

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """Store intermediate writes linked to a checkpoint asynchronously."""
        thread_id, checkpoint_ns, checkpoint_id = self._config_values(
            config, require_checkpoint_id=True
        )
        rows = dump_writes(
            serde=self.serde,
            thread_id=thread_id,
            checkpoint_ns=checkpoint_ns,
            checkpoint_id=cast(str, checkpoint_id),
            task_id=task_id,
            task_path=task_path,
            writes=writes,
        )
        replace_existing = all(channel in WRITES_IDX_MAP for channel, _ in writes)
        statement = merge_write_sql(self.tables, update_existing=replace_existing)

        async with self._thread_guard(thread_id):
            async with self._managed_cursor(write=True) as cursor:
                for row in rows:
                    await self._execute_write_row(
                        cursor,
                        statement,
                        row,
                        ignore_duplicate=not replace_existing,
                    )

    async def adelete_thread(self, thread_id: str) -> None:
        """Delete checkpoints, blobs, and writes for a thread asynchronously."""
        async with self._thread_guard(thread_id):
            async with self._managed_cursor(write=True) as cursor:
                await cursor.execute(
                    f"DELETE FROM {self.tables.writes} WHERE thread_id = :1",
                    (thread_id,),
                )
                await cursor.execute(
                    f"DELETE FROM {self.tables.blobs} WHERE thread_id = :1",
                    (thread_id,),
                )
                await cursor.execute(
                    f"DELETE FROM {self.tables.checkpoints} WHERE thread_id = :1",
                    (thread_id,),
                )

    async def adelete_for_runs(self, run_ids: Sequence[str]) -> None:
        """Delete checkpoints and writes whose metadata belongs to run ids."""
        if not run_ids:
            return
        rows_by_run: list[list[tuple[str, str, str]]] = []
        async with self._managed_cursor(write=False) as cursor:
            for run_id in run_ids:
                rows = await self._checkpoint_keys_for_run(cursor, run_id)
                rows_by_run.append(rows)

        thread_ids = [row[0] for rows in rows_by_run for row in rows]
        async with self._thread_guard(*thread_ids):
            async with self._managed_cursor(write=True) as cursor:
                for rows in rows_by_run:
                    for thread_id, checkpoint_ns, checkpoint_id in rows:
                        await cursor.execute(
                            f"""
                            DELETE FROM {self.tables.writes}
                            WHERE thread_id = :1
                              AND checkpoint_ns = :2
                              AND checkpoint_id = :3
                            """,
                            (thread_id, checkpoint_ns, checkpoint_id),
                        )
                        await cursor.execute(
                            f"""
                            DELETE FROM {self.tables.checkpoints}
                            WHERE thread_id = :1
                              AND checkpoint_ns = :2
                              AND checkpoint_id = :3
                            """,
                            (thread_id, checkpoint_ns, checkpoint_id),
                        )

    async def acopy_thread(self, source_thread_id: str, target_thread_id: str) -> None:
        """Copy all checkpoint rows from one thread id to another."""
        async with self._thread_guard(source_thread_id, target_thread_id):
            async with self._managed_cursor(write=True) as cursor:
                await cursor.execute(
                    f"""
                    INSERT INTO {self.tables.checkpoints}
                        (thread_id, checkpoint_ns, checkpoint_id,
                         parent_checkpoint_id, checkpoint, metadata)
                    SELECT :1, checkpoint_ns, checkpoint_id,
                           parent_checkpoint_id, checkpoint, metadata
                    FROM {self.tables.checkpoints}
                    WHERE thread_id = :2
                    """,
                    (target_thread_id, source_thread_id),
                )
                await cursor.execute(
                    f"""
                    INSERT INTO {self.tables.blobs}
                        (thread_id, checkpoint_ns, channel, version,
                         type_tag, blob_value)
                    SELECT :1, checkpoint_ns, channel, version,
                           type_tag, blob_value
                    FROM {self.tables.blobs}
                    WHERE thread_id = :2
                    """,
                    (target_thread_id, source_thread_id),
                )
                await cursor.execute(
                    f"""
                    INSERT INTO {self.tables.writes}
                        (thread_id, checkpoint_ns, checkpoint_id, task_id,
                         task_path, write_idx, channel, type_tag, blob_value)
                    SELECT :1, checkpoint_ns, checkpoint_id, task_id,
                           task_path, write_idx, channel, type_tag, blob_value
                    FROM {self.tables.writes}
                    WHERE thread_id = :2
                    """,
                    (target_thread_id, source_thread_id),
                )

    async def aprune(
        self,
        thread_ids: Sequence[str],
        *,
        strategy: str = "keep_latest",
    ) -> None:
        """Prune checkpoints for the given threads asynchronously."""
        if strategy == "delete":
            for thread_id in thread_ids:
                await self.adelete_thread(thread_id)
            return
        if strategy == "keep_latest":
            raise OracleADBUnsupportedOperation(
                "keep_latest pruning is not implemented because it must preserve "
                "DeltaChannel ancestor chains."
            )
        raise OracleADBConfigurationError(f"Unsupported prune strategy: {strategy}")

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        """Reject sync access for the async saver."""
        del config
        self._raise_sync_not_supported()

    def list(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,  # pylint: disable=redefined-builtin
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Iterator[CheckpointTuple]:
        """Reject sync listing for the async saver."""
        del config, filter, before, limit
        self._raise_sync_not_supported()
        yield

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """Reject sync writes for the async saver."""
        del config, checkpoint, metadata, new_versions
        self._raise_sync_not_supported()

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """Reject sync pending writes for the async saver."""
        del config, writes, task_id, task_path
        self._raise_sync_not_supported()

    def delete_thread(self, thread_id: str) -> None:
        """Reject sync delete for the async saver."""
        del thread_id
        self._raise_sync_not_supported()

    def delete_for_runs(self, run_ids: Sequence[str]) -> None:
        """Reject sync run deletion for the async saver."""
        del run_ids
        self._raise_sync_not_supported()

    def copy_thread(self, source_thread_id: str, target_thread_id: str) -> None:
        """Reject sync copy for the async saver."""
        del source_thread_id, target_thread_id
        self._raise_sync_not_supported()

    def prune(
        self,
        thread_ids: Sequence[str],
        *,
        strategy: str = "keep_latest",
    ) -> None:
        """Reject sync pruning for the async saver."""
        del thread_ids, strategy
        self._raise_sync_not_supported()

    def get_next_version(self, current: str | None, channel: None) -> str:
        """Generate a sortable LangGraph channel version string."""
        del channel
        if current is None:
            current_version = 0
        elif isinstance(current, int):
            current_version = current
        else:
            current_version = int(str(current).split(".", maxsplit=1)[0])
        return f"{current_version + 1:032}.{random.random():016}"

    @staticmethod
    def _raise_sync_not_supported() -> None:
        """Raise the standard error for sync methods on the async saver."""
        raise OracleADBUnsupportedOperation(
            "AsyncOracleADBCheckpointer is asynchronous. Use LangGraph async "
            "methods such as aget_tuple, alist, aput, and aput_writes."
        )

    @asynccontextmanager
    async def _managed_cursor(self, *, write: bool) -> AsyncIterator[_AsyncCursor]:
        """Yield an async cursor and manage transaction boundaries."""
        connection_context = (
            nullcontext() if self._uses_pool() else self._connection_lock
        )
        async with connection_context:
            acquired_connection = False
            connection = self.conn
            if self._uses_pool():
                connection = self.conn.acquire()
                acquired_connection = True

            cursor = connection.cursor()
            try:
                yield cursor
                if write and self.commit_on_success:
                    await connection.commit()
            except oracledb.Error as exc:
                if write and self.commit_on_success and hasattr(connection, "rollback"):
                    await connection.rollback()
                raise OracleADBDatabaseError("Oracle ADB operation failed.") from exc
            finally:
                cursor.close()
                if acquired_connection:
                    await connection.close()

    @asynccontextmanager
    async def _thread_guard(self, *thread_ids: str) -> AsyncIterator[None]:
        """Serialize same-process async operations that touch thread ids."""
        locks = await self._locks_for_threads(thread_ids)
        for thread_lock in locks:
            await thread_lock.acquire()
        try:
            yield
        finally:
            for thread_lock in reversed(locks):
                thread_lock.release()

    @asynccontextmanager
    async def _read_thread_guard(
        self, config: RunnableConfig | None
    ) -> AsyncIterator[None]:
        """Guard async reads by thread id when the caller supplied one."""
        configurable = config.get("configurable") if config else None
        if configurable and "thread_id" in configurable:
            async with self._thread_guard(str(configurable["thread_id"])):
                yield
            return
        yield

    async def _locks_for_threads(self, thread_ids: Sequence[str]) -> list[asyncio.Lock]:
        """Return stable async locks for thread ids in deadlock-safe order."""
        unique_thread_ids = sorted({str(thread_id) for thread_id in thread_ids})
        async with self._thread_locks_guard:
            return [
                self._thread_locks.setdefault(thread_id, asyncio.Lock())
                for thread_id in unique_thread_ids
            ]

    def _uses_pool(self) -> bool:
        """Return whether the connection-like object behaves as a pool."""
        return hasattr(self.conn, "acquire")

    async def _execute_write_row(
        self,
        cursor: _AsyncCursor,
        statement: str,
        row: tuple[str, str, str, str, str, int, str, str, bytes],
        *,
        ignore_duplicate: bool,
    ) -> None:
        """Execute one async write row and optionally ignore duplicate races."""
        try:
            await cursor.execute(statement, row)
        except oracledb.DatabaseError as exc:
            if ignore_duplicate and self._is_unique_constraint_error(exc):
                return
            raise

    @staticmethod
    def _is_unique_constraint_error(exc: oracledb.DatabaseError) -> bool:
        """Return whether an Oracle error is a unique constraint violation."""
        error = exc.args[0] if exc.args else None
        error_code = getattr(error, "code", None)
        return error_code == 1 or "ORA-00001" in str(exc)

    @staticmethod
    async def _execute_ddl_if_missing(cursor: _AsyncCursor, statement: str) -> None:
        """Execute async DDL and ignore Oracle's existing-object error."""
        try:
            await cursor.execute(statement)
        except oracledb.DatabaseError as exc:
            error = exc.args[0] if exc.args else None
            error_code = getattr(error, "code", None)
            if error_code != 955:
                raise

    @staticmethod
    def _to_json(value: Any) -> str:
        """Serialize a value to compact JSON."""
        return json.dumps(value, separators=(",", ":"), sort_keys=True)

    @staticmethod
    def _from_json(value: Any) -> Any:
        """Deserialize JSON from strings or Oracle LOB values."""
        if hasattr(value, "read"):
            value = value.read()
        return json.loads(value)

    @staticmethod
    def _read_blob(value: Any) -> bytes | None:
        """Read bytes from Oracle BLOB values or plain byte strings."""
        if value is None:
            return None
        if hasattr(value, "read"):
            return value.read()
        return value

    @staticmethod
    def _metadata_matches(
        metadata: CheckpointMetadata, expected: dict[str, Any]
    ) -> bool:
        """Return whether metadata contains all expected key/value pairs."""
        return all(metadata.get(key) == value for key, value in expected.items())

    async def _load_checkpoint_tuple(
        self, cursor: _AsyncCursor, row: Sequence[Any]
    ) -> CheckpointTuple:
        """Convert a checkpoint table row into a LangGraph CheckpointTuple."""
        thread_id, checkpoint_ns, checkpoint_id, parent_id, checkpoint_raw, meta_raw = (
            row
        )
        checkpoint = self._from_json(checkpoint_raw)
        metadata = self._from_json(meta_raw)
        blob_rows = await self._select_blobs_for_checkpoint(
            cursor,
            thread_id=thread_id,
            checkpoint_ns=checkpoint_ns,
            channel_versions=checkpoint.get("channel_versions", {}),
        )
        write_rows = await self._select_writes(
            cursor,
            thread_id=thread_id,
            checkpoint_ns=checkpoint_ns,
            checkpoint_id=checkpoint_id,
        )

        checkpoint["channel_values"] = {
            **(checkpoint.get("channel_values") or {}),
            **load_blobs(self.serde, blob_rows),
        }

        parent_config = (
            {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": parent_id,
                }
            }
            if parent_id
            else None
        )
        return CheckpointTuple(
            {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": checkpoint_id,
                }
            },
            checkpoint,
            metadata,
            parent_config,
            load_writes(self.serde, write_rows),
        )

    async def _select_blobs_for_checkpoint(
        self,
        cursor: _AsyncCursor,
        *,
        thread_id: str,
        checkpoint_ns: str,
        channel_versions: dict[str, Any],
    ) -> list[tuple[str, str, bytes | None]]:
        """Load serialized blobs referenced by one checkpoint."""
        rows: list[tuple[str, str, bytes | None]] = []
        for channel, version in channel_versions.items():
            await cursor.execute(
                select_blob_sql(self.tables),
                (thread_id, checkpoint_ns, channel, str(version)),
            )
            row = await cursor.fetchone()
            if row is not None:
                rows.append((row[0], row[1], self._read_blob(row[2])))
        return rows

    async def _select_writes(
        self,
        cursor: _AsyncCursor,
        *,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str,
    ) -> list[tuple[str, str, str, bytes]]:
        """Load pending writes for one checkpoint."""
        await cursor.execute(
            select_writes_sql(self.tables),
            (thread_id, checkpoint_ns, checkpoint_id),
        )
        return [
            (row[0], row[1], row[2], cast(bytes, self._read_blob(row[3])))
            for row in await cursor.fetchall()
        ]

    async def _checkpoint_keys_for_run(
        self, cursor: _AsyncCursor, run_id: str
    ) -> list[tuple[str, str, str]]:
        """Return checkpoint keys for one LangGraph run id."""
        await cursor.execute(checkpoint_keys_for_run_sql(self.tables), (run_id,))
        return [(row[0], row[1], row[2]) for row in await cursor.fetchall()]

    def _config_values(
        self, config: RunnableConfig, *, require_checkpoint_id: bool = False
    ) -> tuple[str, str, str | None]:
        """Extract required LangGraph config values."""
        configurable = config.get("configurable")
        if not configurable or "thread_id" not in configurable:
            raise OracleADBConfigurationError(
                "LangGraph config must include configurable.thread_id."
            )
        checkpoint_id = get_checkpoint_id(config)
        if require_checkpoint_id and checkpoint_id is None:
            raise OracleADBConfigurationError(
                "LangGraph config must include configurable.checkpoint_id."
            )
        return (
            str(configurable["thread_id"]),
            str(configurable.get("checkpoint_ns", "")),
            checkpoint_id,
        )

    def _list_query(
        self,
        *,
        config: RunnableConfig | None,
        before: RunnableConfig | None,
    ) -> tuple[str, tuple[Any, ...]]:
        """Build the checkpoint listing query and parameters."""
        thread_id: str | None = None
        checkpoint_ns: str | None = None
        checkpoint_id: str | None = None
        if config is not None:
            thread_id, checkpoint_ns, checkpoint_id = self._config_values(config)

        before_id = None
        if before is not None:
            before_id = get_checkpoint_id(before)

        return list_checkpoints_query(
            self.tables,
            thread_id=thread_id,
            checkpoint_ns=checkpoint_ns,
            checkpoint_id=checkpoint_id,
            before_checkpoint_id=before_id,
        )
