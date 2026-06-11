"""
Author: L. Saetta
Date last modified: 2026-06-11
License: MIT
Description: Serialization helpers for Oracle-backed LangGraph checkpoints.
"""

from __future__ import annotations

from typing import Any, Mapping, Sequence, cast

from langgraph.checkpoint.base import (
    WRITES_IDX_MAP,
    ChannelVersions,
    Checkpoint,
    PendingWrite,
)
from langgraph.checkpoint.serde.base import SerializerProtocol
from langgraph.checkpoint.serde.types import _DeltaSnapshot


def split_checkpoint_blobs(
    checkpoint: Checkpoint,
) -> tuple[Checkpoint, dict[str, Any]]:
    """Split checkpoint values into JSON-inline values and serialized blobs.

    LangGraph checkpointers keep primitive channel values inline and move
    complex values to a blob table. This mirrors that behavior for Oracle.

    Args:
        checkpoint: LangGraph checkpoint to persist.

    Returns:
        A tuple containing the JSON-safe checkpoint copy and the channel values
        that must be stored as typed blobs.
    """
    checkpoint_copy = cast(Checkpoint, checkpoint.copy())
    checkpoint_copy["channel_values"] = checkpoint["channel_values"].copy()

    blob_values: dict[str, Any] = {}
    for channel, value in checkpoint["channel_values"].items():
        if isinstance(value, _DeltaSnapshot):
            blob_values[channel] = checkpoint_copy["channel_values"].pop(channel)
            checkpoint_copy["channel_values"][channel] = True
        elif value is None or isinstance(value, (str, int, float, bool)):
            continue
        else:
            blob_values[channel] = checkpoint_copy["channel_values"].pop(channel)

    return checkpoint_copy, blob_values


def dump_blobs(
    *,
    serde: SerializerProtocol,
    thread_id: str,
    checkpoint_ns: str,
    values: Mapping[str, Any],
    versions: ChannelVersions,
) -> list[tuple[str, str, str, str, str, bytes | None]]:
    """Serialize channel values for storage in the blob table.

    Args:
        serde: LangGraph serializer.
        thread_id: LangGraph thread identifier.
        checkpoint_ns: LangGraph checkpoint namespace.
        values: Channel values moved out of checkpoint JSON.
        versions: Channel versions from the checkpoint write.

    Returns:
        Rows ready for Oracle bind variables.
    """
    rows: list[tuple[str, str, str, str, str, bytes | None]] = []
    for channel, version in versions.items():
        if channel not in values:
            continue
        type_tag, payload = serde.dumps_typed(values[channel])
        rows.append(
            (
                thread_id,
                checkpoint_ns,
                channel,
                str(version),
                type_tag,
                payload,
            )
        )
    return rows


def load_blobs(
    serde: SerializerProtocol,
    blob_rows: Sequence[tuple[str, str, bytes | None]],
) -> dict[str, Any]:
    """Deserialize blob rows into checkpoint channel values.

    Args:
        serde: LangGraph serializer.
        blob_rows: Rows shaped as `(channel, type_tag, blob_value)`.

    Returns:
        Mapping from channel name to deserialized value.
    """
    values: dict[str, Any] = {}
    for channel, type_tag, payload in blob_rows:
        if type_tag == "empty" or payload is None:
            continue
        values[channel] = serde.loads_typed((type_tag, payload))
    return values


def dump_writes(  # pylint: disable=too-many-arguments
    *,
    serde: SerializerProtocol,
    thread_id: str,
    checkpoint_ns: str,
    checkpoint_id: str,
    task_id: str,
    task_path: str,
    writes: Sequence[tuple[str, Any]],
) -> list[tuple[str, str, str, str, str, int, str, str, bytes]]:
    """Serialize pending writes for storage.

    Args:
        serde: LangGraph serializer.
        thread_id: LangGraph thread identifier.
        checkpoint_ns: LangGraph checkpoint namespace.
        checkpoint_id: LangGraph checkpoint identifier.
        task_id: LangGraph task identifier.
        task_path: LangGraph task path.
        writes: Pending writes.

    Returns:
        Rows ready for Oracle bind variables.
    """
    rows: list[tuple[str, str, str, str, str, int, str, str, bytes]] = []
    for index, (channel, value) in enumerate(writes):
        type_tag, payload = serde.dumps_typed(value)
        rows.append(
            (
                thread_id,
                checkpoint_ns,
                checkpoint_id,
                task_id,
                task_path,
                WRITES_IDX_MAP.get(channel, index),
                channel,
                type_tag,
                payload,
            )
        )
    return rows


def load_writes(
    serde: SerializerProtocol,
    write_rows: Sequence[tuple[str, str, bytes]],
) -> list[PendingWrite]:
    """Deserialize stored write rows into LangGraph pending writes.

    Args:
        serde: LangGraph serializer.
        write_rows: Rows shaped as `(task_id, channel, type_tag, blob_value)`.

    Returns:
        Pending writes in LangGraph tuple form.
    """
    return [
        (task_id, channel, serde.loads_typed((type_tag, payload)))
        for task_id, channel, type_tag, payload in write_rows
    ]
