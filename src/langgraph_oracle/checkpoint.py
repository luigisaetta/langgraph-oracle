"""
Author: L. Saetta
Date last modified: 2026-06-11
License: MIT
Description: LangGraph checkpointer implementation backed by Oracle ADB.
"""

from __future__ import annotations

import json
import random
import threading
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
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


class _Cursor(Protocol):
    """Protocol for the small cursor surface used by the checkpointer."""

    def execute(self, statement: str, parameters: Any = None) -> Any:
        """Execute one SQL statement."""

    def executemany(self, statement: str, parameters: Sequence[Any]) -> Any:
        """Execute one SQL statement for many parameter sets."""

    def fetchone(self) -> Any:
        """Fetch one row."""

    def fetchall(self) -> list[Any]:
        """Fetch all rows."""


class OracleADBCheckpointer(BaseCheckpointSaver[str]):
    """LangGraph checkpointer backed by Oracle Autonomous Database.

    The implementation uses only `oracledb` for database access. Schema creation
    is explicit: call `setup()` once before using the saver.
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
        """Initialize the Oracle ADB checkpointer.

        Args:
            conn: Existing `oracledb.Connection` or `oracledb.ConnectionPool`.
            table_prefix: Prefix for checkpointer tables.
            serde: Optional LangGraph serializer.
            commit_on_success: Commit write transactions after successful
                operations. Set to `False` when transaction ownership belongs to
                the caller.
            close_on_exit: Close the connection or pool when the saver exits its
                context manager.

        Raises:
            OracleADBConfigurationError: If the connection object is missing.
        """
        if conn is None:
            raise OracleADBConfigurationError("An oracledb connection is required.")

        super().__init__(serde=serde)
        self.conn = conn
        self.tables = OracleCheckpointTables.from_prefix(table_prefix)
        self.commit_on_success = commit_on_success
        self.close_on_exit = close_on_exit
        self._lock = threading.RLock()

    @classmethod
    @contextmanager
    def from_connection_params(
        cls,
        *,
        table_prefix: str = "LG",
        serde: SerializerProtocol | None = None,
        commit_on_success: bool = True,
        **connect_kwargs: Any,
    ) -> Iterator["OracleADBCheckpointer"]:
        """Create a checkpointer that owns an `oracledb` connection.

        Args:
            table_prefix: Prefix for checkpointer tables.
            serde: Optional LangGraph serializer.
            commit_on_success: Commit write transactions after successful
                operations.
            **connect_kwargs: Arguments passed directly to `oracledb.connect`.

        Yields:
            An Oracle ADB checkpointer.
        """
        connection = oracledb.connect(**connect_kwargs)
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
            saver.close()

    def __enter__(self) -> "OracleADBCheckpointer":
        """Return this saver when used as a context manager."""
        return self

    def __exit__(self, *_exc_info: object) -> None:
        """Close the underlying resource when configured to do so."""
        self.close()

    def close(self) -> None:
        """Close the connection or pool if this saver owns it."""
        if self.close_on_exit and hasattr(self.conn, "close"):
            self.conn.close()

    def setup(self) -> None:
        """Create or migrate the Oracle checkpoint schema.

        Raises:
            OracleADBDatabaseError: If setup fails.
        """
        with self._managed_cursor(write=True) as cursor:
            for statement in create_table_statements(self.tables):
                self._execute_ddl_if_missing(cursor, statement)
            for statement in create_index_statements(self.tables):
                self._execute_ddl_if_missing(cursor, statement)
            cursor.execute(
                f"""
                MERGE INTO {self.tables.migrations} target
                USING (SELECT :1 AS version_number FROM dual) source
                ON (target.version_number = source.version_number)
                WHEN NOT MATCHED THEN
                    INSERT (version_number) VALUES (source.version_number)
                """,
                (0,),
            )

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        """Fetch a checkpoint tuple using LangGraph configuration.

        Args:
            config: Runnable config containing `thread_id` and optional
                `checkpoint_ns` and `checkpoint_id`.

        Returns:
            Matching checkpoint tuple, or `None` if no row exists.
        """
        thread_id, checkpoint_ns, checkpoint_id = self._config_values(config)
        if checkpoint_id is None:
            statement = f"""
                SELECT thread_id, checkpoint_ns, checkpoint_id,
                       parent_checkpoint_id, checkpoint, metadata
                FROM {self.tables.checkpoints}
                WHERE thread_id = :1 AND checkpoint_ns = :2
                ORDER BY checkpoint_id DESC
                FETCH FIRST 1 ROWS ONLY
            """
            parameters = (thread_id, checkpoint_ns)
        else:
            statement = f"""
                SELECT thread_id, checkpoint_ns, checkpoint_id,
                       parent_checkpoint_id, checkpoint, metadata
                FROM {self.tables.checkpoints}
                WHERE thread_id = :1
                  AND checkpoint_ns = :2
                  AND checkpoint_id = :3
            """
            parameters = (thread_id, checkpoint_ns, checkpoint_id)

        with self._managed_cursor(write=False) as cursor:
            cursor.execute(statement, parameters)
            row = cursor.fetchone()
            if row is None:
                return None
            return self._load_checkpoint_tuple(cursor, row)

    def list(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,  # pylint: disable=redefined-builtin
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Iterator[CheckpointTuple]:
        """List checkpoints matching LangGraph filters.

        Args:
            config: Optional base config containing `thread_id` and namespace.
            filter: Metadata key/value filter.
            before: Optional config whose checkpoint id acts as an exclusive
                upper bound.
            limit: Maximum number of checkpoint tuples to yield.

        Yields:
            Matching checkpoint tuples, newest first.
        """
        statement, parameters = self._list_query(config=config, before=before)
        yielded = 0

        with self._managed_cursor(write=False) as cursor:
            cursor.execute(statement, parameters)
            for row in cursor.fetchall():
                checkpoint_tuple = self._load_checkpoint_tuple(cursor, row)
                if filter and not self._metadata_matches(
                    checkpoint_tuple.metadata, filter
                ):
                    continue
                yield checkpoint_tuple
                yielded += 1
                if limit is not None and yielded >= limit:
                    break

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """Store a LangGraph checkpoint.

        Args:
            config: Runnable config for the current checkpoint write.
            checkpoint: Checkpoint payload.
            metadata: LangGraph checkpoint metadata.
            new_versions: Channel versions written by this checkpoint.

        Returns:
            Runnable config pointing at the stored checkpoint.
        """
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

        with self._managed_cursor(write=True) as cursor:
            for row in blob_rows:
                cursor.execute(self._merge_blob_sql(), row)
            cursor.execute(
                self._merge_checkpoint_sql(),
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

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """Store intermediate writes linked to a checkpoint.

        Args:
            config: Runnable config containing the target checkpoint id.
            writes: LangGraph writes to persist.
            task_id: Task identifier.
            task_path: Task path.
        """
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
        statement = self._merge_write_sql(update_existing=replace_existing)

        with self._managed_cursor(write=True) as cursor:
            for row in rows:
                cursor.execute(statement, row)

    def delete_thread(self, thread_id: str) -> None:
        """Delete checkpoints, blobs, and writes for a thread.

        Args:
            thread_id: LangGraph thread identifier.
        """
        with self._managed_cursor(write=True) as cursor:
            cursor.execute(
                f"DELETE FROM {self.tables.writes} WHERE thread_id = :1", (thread_id,)
            )
            cursor.execute(
                f"DELETE FROM {self.tables.blobs} WHERE thread_id = :1", (thread_id,)
            )
            cursor.execute(
                f"DELETE FROM {self.tables.checkpoints} WHERE thread_id = :1",
                (thread_id,),
            )

    def delete_for_runs(self, run_ids: Sequence[str]) -> None:
        """Delete checkpoints and writes whose metadata belongs to run ids.

        Args:
            run_ids: LangGraph run identifiers.
        """
        if not run_ids:
            return
        with self._managed_cursor(write=True) as cursor:
            for run_id in run_ids:
                rows = self._checkpoint_keys_for_run(cursor, run_id)
                for thread_id, checkpoint_ns, checkpoint_id in rows:
                    cursor.execute(
                        f"""
                        DELETE FROM {self.tables.writes}
                        WHERE thread_id = :1
                          AND checkpoint_ns = :2
                          AND checkpoint_id = :3
                        """,
                        (thread_id, checkpoint_ns, checkpoint_id),
                    )
                    cursor.execute(
                        f"""
                        DELETE FROM {self.tables.checkpoints}
                        WHERE thread_id = :1
                          AND checkpoint_ns = :2
                          AND checkpoint_id = :3
                        """,
                        (thread_id, checkpoint_ns, checkpoint_id),
                    )

    def copy_thread(self, source_thread_id: str, target_thread_id: str) -> None:
        """Copy all checkpoint rows from one thread id to another.

        Args:
            source_thread_id: Source LangGraph thread id.
            target_thread_id: Target LangGraph thread id.
        """
        with self._managed_cursor(write=True) as cursor:
            cursor.execute(
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
            cursor.execute(
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
            cursor.execute(
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

    def prune(
        self,
        thread_ids: Sequence[str],
        *,
        strategy: str = "keep_latest",
    ) -> None:
        """Prune checkpoints for the given threads.

        Args:
            thread_ids: Thread ids to prune.
            strategy: `"delete"` removes all state. `"keep_latest"` is not yet
                implemented because unsafe pruning can break delta channels.

        Raises:
            OracleADBUnsupportedOperation: If `keep_latest` is requested.
        """
        if strategy == "delete":
            for thread_id in thread_ids:
                self.delete_thread(thread_id)
            return
        if strategy == "keep_latest":
            raise OracleADBUnsupportedOperation(
                "keep_latest pruning is not implemented because it must preserve "
                "DeltaChannel ancestor chains."
            )
        raise OracleADBConfigurationError(f"Unsupported prune strategy: {strategy}")

    def get_next_version(self, current: str | None, channel: None) -> str:
        """Generate a sortable LangGraph channel version string.

        Args:
            current: Current channel version.
            channel: Deprecated LangGraph argument.

        Returns:
            Next channel version string.
        """
        del channel
        if current is None:
            current_version = 0
        elif isinstance(current, int):
            current_version = current
        else:
            current_version = int(str(current).split(".", maxsplit=1)[0])
        return f"{current_version + 1:032}.{random.random():016}"

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        """Reject async access until a native async saver is implemented.

        Args:
            config: Runnable config.

        Raises:
            OracleADBUnsupportedOperation: Always for this synchronous saver.
        """
        del config
        self._raise_async_not_supported()

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,  # pylint: disable=redefined-builtin
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Any:
        """Reject async listing until a native async saver is implemented.

        Args:
            config: Runnable config.
            filter: Metadata filter.
            before: Optional checkpoint upper bound.
            limit: Maximum result count.

        Raises:
            OracleADBUnsupportedOperation: Always for this synchronous saver.
        """
        del config, filter, before, limit
        self._raise_async_not_supported()
        yield

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """Reject async writes until a native async saver is implemented.

        Args:
            config: Runnable config.
            checkpoint: Checkpoint payload.
            metadata: Checkpoint metadata.
            new_versions: New channel versions.

        Raises:
            OracleADBUnsupportedOperation: Always for this synchronous saver.
        """
        del config, checkpoint, metadata, new_versions
        self._raise_async_not_supported()

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """Reject async pending writes until a native async saver is implemented.

        Args:
            config: Runnable config.
            writes: Pending writes.
            task_id: Task identifier.
            task_path: Task path.

        Raises:
            OracleADBUnsupportedOperation: Always for this synchronous saver.
        """
        del config, writes, task_id, task_path
        self._raise_async_not_supported()

    async def adelete_thread(self, thread_id: str) -> None:
        """Reject async delete until a native async saver is implemented.

        Args:
            thread_id: LangGraph thread identifier.

        Raises:
            OracleADBUnsupportedOperation: Always for this synchronous saver.
        """
        del thread_id
        self._raise_async_not_supported()

    async def adelete_for_runs(self, run_ids: Sequence[str]) -> None:
        """Reject async run deletion until a native async saver is implemented.

        Args:
            run_ids: LangGraph run identifiers.

        Raises:
            OracleADBUnsupportedOperation: Always for this synchronous saver.
        """
        del run_ids
        self._raise_async_not_supported()

    async def acopy_thread(self, source_thread_id: str, target_thread_id: str) -> None:
        """Reject async copy until a native async saver is implemented.

        Args:
            source_thread_id: Source LangGraph thread id.
            target_thread_id: Target LangGraph thread id.

        Raises:
            OracleADBUnsupportedOperation: Always for this synchronous saver.
        """
        del source_thread_id, target_thread_id
        self._raise_async_not_supported()

    async def aprune(
        self,
        thread_ids: Sequence[str],
        *,
        strategy: str = "keep_latest",
    ) -> None:
        """Reject async pruning until a native async saver is implemented.

        Args:
            thread_ids: Thread identifiers.
            strategy: Prune strategy.

        Raises:
            OracleADBUnsupportedOperation: Always for this synchronous saver.
        """
        del thread_ids, strategy
        self._raise_async_not_supported()

    @staticmethod
    def _raise_async_not_supported() -> None:
        """Raise the standard error for async methods on the sync saver."""
        raise OracleADBUnsupportedOperation(
            "OracleADBCheckpointer is synchronous. A native async saver must use "
            "oracledb async connections directly and is intentionally separate."
        )

    @contextmanager
    def _managed_cursor(self, *, write: bool) -> Iterator[_Cursor]:
        """Yield a cursor from a connection or pool and manage transactions."""
        with self._lock:
            acquired_connection = False
            connection = self.conn
            if hasattr(self.conn, "acquire"):
                connection = self.conn.acquire()
                acquired_connection = True

            cursor = connection.cursor()
            try:
                yield cursor
                if write and self.commit_on_success:
                    connection.commit()
            except oracledb.Error as exc:
                if write and self.commit_on_success and hasattr(connection, "rollback"):
                    connection.rollback()
                raise OracleADBDatabaseError("Oracle ADB operation failed.") from exc
            finally:
                cursor.close()
                if acquired_connection:
                    connection.close()

    @staticmethod
    def _execute_ddl_if_missing(cursor: _Cursor, statement: str) -> None:
        """Execute DDL and ignore Oracle's already-existing object error."""
        try:
            cursor.execute(statement)
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

    def _load_checkpoint_tuple(
        self, cursor: _Cursor, row: Sequence[Any]
    ) -> CheckpointTuple:
        """Convert a checkpoint table row into a LangGraph CheckpointTuple."""
        thread_id, checkpoint_ns, checkpoint_id, parent_id, checkpoint_raw, meta_raw = (
            row
        )
        checkpoint = self._from_json(checkpoint_raw)
        metadata = self._from_json(meta_raw)
        blob_rows = self._select_blobs_for_checkpoint(
            cursor,
            thread_id=thread_id,
            checkpoint_ns=checkpoint_ns,
            channel_versions=checkpoint.get("channel_versions", {}),
        )
        write_rows = self._select_writes(
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

    def _select_blobs_for_checkpoint(
        self,
        cursor: _Cursor,
        *,
        thread_id: str,
        checkpoint_ns: str,
        channel_versions: dict[str, Any],
    ) -> list[tuple[str, str, bytes | None]]:
        """Load serialized blobs referenced by one checkpoint."""
        rows: list[tuple[str, str, bytes | None]] = []
        for channel, version in channel_versions.items():
            cursor.execute(
                f"""
                SELECT channel, type_tag, blob_value
                FROM {self.tables.blobs}
                WHERE thread_id = :1
                  AND checkpoint_ns = :2
                  AND channel = :3
                  AND version = :4
                """,
                (thread_id, checkpoint_ns, channel, str(version)),
            )
            row = cursor.fetchone()
            if row is not None:
                rows.append((row[0], row[1], self._read_blob(row[2])))
        return rows

    def _select_writes(
        self,
        cursor: _Cursor,
        *,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str,
    ) -> list[tuple[str, str, str, bytes]]:
        """Load pending writes for one checkpoint."""
        cursor.execute(
            f"""
            SELECT task_id, channel, type_tag, blob_value
            FROM {self.tables.writes}
            WHERE thread_id = :1
              AND checkpoint_ns = :2
              AND checkpoint_id = :3
            ORDER BY task_id, write_idx
            """,
            (thread_id, checkpoint_ns, checkpoint_id),
        )
        return [
            (row[0], row[1], row[2], cast(bytes, self._read_blob(row[3])))
            for row in cursor.fetchall()
        ]

    def _checkpoint_keys_for_run(
        self, cursor: _Cursor, run_id: str
    ) -> list[tuple[str, str, str]]:
        """Return checkpoint keys for one LangGraph run id."""
        cursor.execute(
            f"""
            SELECT thread_id, checkpoint_ns, checkpoint_id
            FROM {self.tables.checkpoints}
            WHERE JSON_VALUE(metadata, '$.run_id') = :1
            """,
            (run_id,),
        )
        return [(row[0], row[1], row[2]) for row in cursor.fetchall()]

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
        clauses: list[str] = []
        parameters: list[Any] = []
        if config is not None:
            thread_id, checkpoint_ns, checkpoint_id = self._config_values(config)
            clauses.append("thread_id = :1")
            parameters.append(thread_id)
            clauses.append(f"checkpoint_ns = :{len(parameters) + 1}")
            parameters.append(checkpoint_ns)
            if checkpoint_id is not None:
                clauses.append(f"checkpoint_id = :{len(parameters) + 1}")
                parameters.append(checkpoint_id)

        if before is not None:
            before_id = get_checkpoint_id(before)
            if before_id is not None:
                clauses.append(f"checkpoint_id < :{len(parameters) + 1}")
                parameters.append(before_id)

        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return (
            f"""
            SELECT thread_id, checkpoint_ns, checkpoint_id,
                   parent_checkpoint_id, checkpoint, metadata
            FROM {self.tables.checkpoints}
            {where_clause}
            ORDER BY checkpoint_id DESC
            """,
            tuple(parameters),
        )

    def _merge_checkpoint_sql(self) -> str:
        """Return Oracle MERGE SQL for checkpoint rows."""
        return f"""
            MERGE INTO {self.tables.checkpoints} target
            USING (
                SELECT :1 AS thread_id,
                       :2 AS checkpoint_ns,
                       :3 AS checkpoint_id,
                       :4 AS parent_checkpoint_id,
                       :5 AS checkpoint,
                       :6 AS metadata
                FROM dual
            ) source
            ON (
                target.thread_id = source.thread_id
                AND target.checkpoint_ns = source.checkpoint_ns
                AND target.checkpoint_id = source.checkpoint_id
            )
            WHEN MATCHED THEN UPDATE SET
                target.parent_checkpoint_id = source.parent_checkpoint_id,
                target.checkpoint = source.checkpoint,
                target.metadata = source.metadata
            WHEN NOT MATCHED THEN INSERT (
                thread_id, checkpoint_ns, checkpoint_id,
                parent_checkpoint_id, checkpoint, metadata
            ) VALUES (
                source.thread_id, source.checkpoint_ns, source.checkpoint_id,
                source.parent_checkpoint_id, source.checkpoint, source.metadata
            )
        """

    def _merge_blob_sql(self) -> str:
        """Return Oracle MERGE SQL for serialized channel blobs."""
        return f"""
            MERGE INTO {self.tables.blobs} target
            USING (
                SELECT :1 AS thread_id,
                       :2 AS checkpoint_ns,
                       :3 AS channel,
                       :4 AS version,
                       :5 AS type_tag,
                       :6 AS blob_value
                FROM dual
            ) source
            ON (
                target.thread_id = source.thread_id
                AND target.checkpoint_ns = source.checkpoint_ns
                AND target.channel = source.channel
                AND target.version = source.version
            )
            WHEN NOT MATCHED THEN INSERT (
                thread_id, checkpoint_ns, channel, version, type_tag, blob_value
            ) VALUES (
                source.thread_id, source.checkpoint_ns, source.channel,
                source.version, source.type_tag, source.blob_value
            )
        """

    def _merge_write_sql(self, *, update_existing: bool) -> str:
        """Return Oracle MERGE SQL for pending writes."""
        update_clause = (
            """
            WHEN MATCHED THEN UPDATE SET
                target.channel = source.channel,
                target.type_tag = source.type_tag,
                target.blob_value = source.blob_value
            """
            if update_existing
            else ""
        )
        return f"""
            MERGE INTO {self.tables.writes} target
            USING (
                SELECT :1 AS thread_id,
                       :2 AS checkpoint_ns,
                       :3 AS checkpoint_id,
                       :4 AS task_id,
                       :5 AS task_path,
                       :6 AS write_idx,
                       :7 AS channel,
                       :8 AS type_tag,
                       :9 AS blob_value
                FROM dual
            ) source
            ON (
                target.thread_id = source.thread_id
                AND target.checkpoint_ns = source.checkpoint_ns
                AND target.checkpoint_id = source.checkpoint_id
                AND target.task_id = source.task_id
                AND target.write_idx = source.write_idx
            )
            {update_clause}
            WHEN NOT MATCHED THEN INSERT (
                thread_id, checkpoint_ns, checkpoint_id, task_id,
                task_path, write_idx, channel, type_tag, blob_value
            ) VALUES (
                source.thread_id, source.checkpoint_ns, source.checkpoint_id,
                source.task_id, source.task_path, source.write_idx,
                source.channel, source.type_tag, source.blob_value
            )
        """
