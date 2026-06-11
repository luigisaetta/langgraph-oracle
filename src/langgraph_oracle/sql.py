"""
Author: L. Saetta
Date last modified: 2026-06-11
License: MIT
Description: SQL statement builders for the Oracle ADB LangGraph checkpointer.
"""

from __future__ import annotations

from typing import Any

from langgraph_oracle.schema import OracleCheckpointTables


def setup_migration_sql(tables: OracleCheckpointTables) -> str:
    """Return SQL that records the initial schema migration."""
    return f"""
        MERGE INTO {tables.migrations} target
        USING (SELECT :1 AS version_number FROM dual) source
        ON (target.version_number = source.version_number)
        WHEN NOT MATCHED THEN
            INSERT (version_number) VALUES (source.version_number)
    """


def latest_checkpoint_sql(tables: OracleCheckpointTables) -> str:
    """Return SQL for loading the latest checkpoint in a namespace."""
    return f"""
        SELECT thread_id, checkpoint_ns, checkpoint_id,
               parent_checkpoint_id, checkpoint, metadata
        FROM {tables.checkpoints}
        WHERE thread_id = :1 AND checkpoint_ns = :2
        ORDER BY checkpoint_id DESC
        FETCH FIRST 1 ROWS ONLY
    """


def checkpoint_by_id_sql(tables: OracleCheckpointTables) -> str:
    """Return SQL for loading one checkpoint by id."""
    return f"""
        SELECT thread_id, checkpoint_ns, checkpoint_id,
               parent_checkpoint_id, checkpoint, metadata
        FROM {tables.checkpoints}
        WHERE thread_id = :1
          AND checkpoint_ns = :2
          AND checkpoint_id = :3
    """


def list_checkpoints_query(
    tables: OracleCheckpointTables,
    *,
    thread_id: str | None,
    checkpoint_ns: str | None,
    checkpoint_id: str | None,
    before_checkpoint_id: str | None,
) -> tuple[str, tuple[Any, ...]]:
    """Build the checkpoint listing query and parameters."""
    clauses: list[str] = []
    parameters: list[Any] = []

    if thread_id is not None:
        clauses.append("thread_id = :1")
        parameters.append(thread_id)
    if checkpoint_ns is not None:
        clauses.append(f"checkpoint_ns = :{len(parameters) + 1}")
        parameters.append(checkpoint_ns)
    if checkpoint_id is not None:
        clauses.append(f"checkpoint_id = :{len(parameters) + 1}")
        parameters.append(checkpoint_id)
    if before_checkpoint_id is not None:
        clauses.append(f"checkpoint_id < :{len(parameters) + 1}")
        parameters.append(before_checkpoint_id)

    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return (
        f"""
        SELECT thread_id, checkpoint_ns, checkpoint_id,
               parent_checkpoint_id, checkpoint, metadata
        FROM {tables.checkpoints}
        {where_clause}
        ORDER BY checkpoint_id DESC
        """,
        tuple(parameters),
    )


def merge_checkpoint_sql(tables: OracleCheckpointTables) -> str:
    """Return Oracle MERGE SQL for checkpoint rows."""
    return f"""
        MERGE INTO {tables.checkpoints} target
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


def merge_blob_sql(tables: OracleCheckpointTables) -> str:
    """Return Oracle MERGE SQL for serialized channel blobs."""
    return f"""
        MERGE INTO {tables.blobs} target
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


def merge_write_sql(tables: OracleCheckpointTables, *, update_existing: bool) -> str:
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
        MERGE INTO {tables.writes} target
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


def select_blob_sql(tables: OracleCheckpointTables) -> str:
    """Return SQL for loading one serialized channel blob."""
    return f"""
        SELECT channel, type_tag, blob_value
        FROM {tables.blobs}
        WHERE thread_id = :1
          AND checkpoint_ns = :2
          AND channel = :3
          AND version = :4
    """


def select_writes_sql(tables: OracleCheckpointTables) -> str:
    """Return SQL for loading pending writes for one checkpoint."""
    return f"""
        SELECT task_id, channel, type_tag, blob_value
        FROM {tables.writes}
        WHERE thread_id = :1
          AND checkpoint_ns = :2
          AND checkpoint_id = :3
        ORDER BY task_id, write_idx
    """


def checkpoint_keys_for_run_sql(tables: OracleCheckpointTables) -> str:
    """Return SQL for finding checkpoints that belong to a run id."""
    return f"""
        SELECT thread_id, checkpoint_ns, checkpoint_id
        FROM {tables.checkpoints}
        WHERE JSON_VALUE(metadata, '$.run_id') = :1
    """
