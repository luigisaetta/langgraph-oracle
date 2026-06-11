"""
Author: L. Saetta
Date last modified: 2026-06-11
License: MIT
Description: Tests for checkpoint serialization helpers.
"""

from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

from langgraph_oracle.serialization import (
    dump_blobs,
    dump_writes,
    load_blobs,
    load_writes,
    split_checkpoint_blobs,
)


def test_split_checkpoint_blobs_keeps_primitives_inline() -> None:
    """Verify that primitive channel values remain in checkpoint JSON."""
    checkpoint = {
        "v": 4,
        "id": "checkpoint-1",
        "ts": "2026-06-11T00:00:00+00:00",
        "channel_values": {"name": "value", "count": 1, "items": ["a"]},
        "channel_versions": {"name": "1", "count": "1", "items": "1"},
        "versions_seen": {},
        "updated_channels": None,
    }

    checkpoint_copy, blob_values = split_checkpoint_blobs(checkpoint)

    assert checkpoint_copy["channel_values"] == {"name": "value", "count": 1}
    assert blob_values == {"items": ["a"]}


def test_dump_and_load_blobs_roundtrip_complex_values() -> None:
    """Verify typed blob serialization for non-primitive channel values."""
    serde = JsonPlusSerializer()
    rows = dump_blobs(
        serde=serde,
        thread_id="thread-1",
        checkpoint_ns="",
        values={"items": ["a", "b"]},
        versions={"items": "0001"},
    )

    loaded = load_blobs(serde, [(rows[0][2], rows[0][4], rows[0][5])])

    assert loaded == {"items": ["a", "b"]}


def test_dump_and_load_writes_roundtrip_values() -> None:
    """Verify pending write serialization and deserialization."""
    serde = JsonPlusSerializer()
    rows = dump_writes(
        serde=serde,
        thread_id="thread-1",
        checkpoint_ns="",
        checkpoint_id="checkpoint-1",
        task_id="task-1",
        task_path="",
        writes=[("messages", {"text": "hello"})],
    )

    loaded = load_writes(serde, [(rows[0][3], rows[0][6], rows[0][7], rows[0][8])])

    assert loaded == [("task-1", "messages", {"text": "hello"})]
