# Oracle ADB LangGraph Checkpointer Specification

## Status

Draft.

## Purpose

Create a LangGraph checkpointer backed by Oracle Autonomous Database (ADB).

The implementation must be fully compatible with LangGraph checkpoint semantics and must use only the official `oracledb` Python package for Oracle Database access. No SQLAlchemy, ORM, OCI SDK, or third-party Oracle database wrapper may be used.

## Scope

In scope:

- A persistent LangGraph checkpointer for Oracle Autonomous Database.
- Support for the LangGraph `BaseCheckpointSaver` public contract.
- Synchronous and asynchronous saver APIs where supported by LangGraph and `oracledb`.
- Database setup and schema migration helpers.
- Unit tests that do not require live OCI resources.
- Integration tests for Oracle ADB, clearly marked and skipped unless credentials are provided.
- Example usage with a minimal LangGraph application.

Out of scope for the first implementation:

- Non-ADB Oracle Database targets.
- OCI resource provisioning.
- Application-specific agent logic.
- UI components.
- Database access libraries other than `oracledb`.

## Target Dependencies

Runtime dependencies:

- `langgraph`
- `oracledb`

Development and test dependencies:

- `black`
- `pylint`
- `pytest`
- `pytest-cov`

All tests must run in the `langgraph-oracle` Conda environment.

## LangGraph Compatibility Requirements

The checkpointer must subclass or otherwise conform exactly to LangGraph's `BaseCheckpointSaver` contract.

The implementation must support:

- `get(config)`
- `get_tuple(config)`
- `list(config, *, filter=None, before=None, limit=None)`
- `put(config, checkpoint, metadata, new_versions)`
- `put_writes(config, writes, task_id, task_path="")`
- `delete_thread(thread_id)`
- `delete_for_runs(run_ids)`
- `copy_thread(source_thread_id, target_thread_id)`
- `prune(thread_ids, *, strategy="keep_latest")`
- `get_next_version(current, channel)`

The asynchronous API must be implemented when the selected `oracledb` version supports the required asynchronous primitives:

- `aget(config)`
- `aget_tuple(config)`
- `alist(config, *, filter=None, before=None, limit=None)`
- `aput(config, checkpoint, metadata, new_versions)`
- `aput_writes(config, writes, task_id, task_path="")`
- `adelete_thread(thread_id)`
- `adelete_for_runs(run_ids)`
- `acopy_thread(source_thread_id, target_thread_id)`
- `aprune(thread_ids, *, strategy="keep_latest")`

If an async method cannot be implemented without blocking, the implementation must document the limitation before code is added. Blocking async wrappers are not acceptable.

The checkpointer must preserve LangGraph behavior for:

- `thread_id`
- `checkpoint_ns`
- `checkpoint_id`
- `parent_config`
- `checkpoint`
- `metadata`
- `new_versions`
- `pending_writes`
- `task_id`
- `task_path`
- special write indexes defined by LangGraph for error, scheduled, interrupt, and resume writes
- delta channel reconstruction behavior

The implementation must return `CheckpointTuple` instances matching LangGraph expectations.

## Public API

The expected primary class name is:

```python
OracleADBCheckpointer
```

The expected async class name is:

```python
AsyncOracleADBCheckpointer
```

The class should accept either an existing `oracledb` connection/pool object or connection settings sufficient to create one.

Expected construction patterns:

```python
from langgraph_oracle.checkpoint import OracleADBCheckpointer

checkpointer = OracleADBCheckpointer(conn=connection)
```

```python
from langgraph_oracle.checkpoint import OracleADBCheckpointer

with OracleADBCheckpointer.from_connection_params(
    user="LANGGRAPH_CHECKPOINT",
    password=password,
    dsn="mydb_low",
    config_dir="/path/to/unzipped/wallet",
    wallet_location="/path/to/unzipped/wallet",
    wallet_password=wallet_password,
) as checkpointer:
    checkpointer.setup()
```

The final API may be refined during implementation, but it must remain explicit about ownership of connections and pools.

## Oracle ADB Connectivity

The implementation must use `oracledb` directly.

Supported connection inputs:

- Existing `oracledb.Connection`
- Existing `oracledb.ConnectionPool`
- Existing `oracledb.AsyncConnection`
- Existing `oracledb.AsyncConnectionPool`
- Connection parameters passed to `oracledb.connect`
- Pool parameters passed to `oracledb.create_pool`
- Async connection or pool parameters passed to `oracledb.connect_async` or `oracledb.create_pool_async`

ADB wallet-based configuration must support:

- `dsn`
- `config_dir`
- `wallet_location`
- `wallet_password`
- `user`
- `password`

Credentials and wallet paths must never be hard-coded.

The checkpointer must be used with a dedicated database schema owner for checkpointing. It must not use the ADB administrative schema in examples, tests, or documentation. The dedicated schema owns the checkpointer tables and indexes created by `setup()`.

## Database Schema

The schema must be created by an explicit `setup()` method. The checkpointer must not silently create or migrate tables during normal checkpoint operations.

Tables should use a configurable prefix. The default prefix should be `LG`.

Initial logical tables:

- `LG_CHECKPOINT_MIGRATIONS`
- `LG_CHECKPOINTS`
- `LG_CHECKPOINT_BLOBS`
- `LG_CHECKPOINT_WRITES`

`LG_CHECKPOINT_MIGRATIONS`:

- `version_number NUMBER(10) PRIMARY KEY`
- `applied_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL`

`LG_CHECKPOINTS`:

- `thread_id VARCHAR2(512) NOT NULL`
- `checkpoint_ns VARCHAR2(512) DEFAULT '' NOT NULL`
- `checkpoint_id VARCHAR2(128) NOT NULL`
- `parent_checkpoint_id VARCHAR2(128)`
- `checkpoint CLOB NOT NULL`
- `metadata CLOB DEFAULT '{}' NOT NULL`
- `created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL`
- primary key: `(thread_id, checkpoint_ns, checkpoint_id)`
- JSON validation constraints for `checkpoint` and `metadata`

`LG_CHECKPOINT_BLOBS`:

- `thread_id VARCHAR2(512) NOT NULL`
- `checkpoint_ns VARCHAR2(512) DEFAULT '' NOT NULL`
- `channel VARCHAR2(512) NOT NULL`
- `version VARCHAR2(128) NOT NULL`
- `type_tag VARCHAR2(256) NOT NULL`
- `blob_value BLOB`
- primary key: `(thread_id, checkpoint_ns, channel, version)`

`LG_CHECKPOINT_WRITES`:

- `thread_id VARCHAR2(512) NOT NULL`
- `checkpoint_ns VARCHAR2(512) DEFAULT '' NOT NULL`
- `checkpoint_id VARCHAR2(128) NOT NULL`
- `task_id VARCHAR2(512) NOT NULL`
- `task_path VARCHAR2(1024) DEFAULT '' NOT NULL`
- `write_idx NUMBER(10) NOT NULL`
- `channel VARCHAR2(512) NOT NULL`
- `type_tag VARCHAR2(256)`
- `blob_value BLOB NOT NULL`
- primary key: `(thread_id, checkpoint_ns, checkpoint_id, task_id, write_idx)`

Required indexes:

- `LG_CHECKPOINTS(thread_id)`
- `LG_CHECKPOINT_BLOBS(thread_id)`
- `LG_CHECKPOINT_WRITES(thread_id)`
- `LG_CHECKPOINTS(thread_id, checkpoint_ns, checkpoint_id DESC)`

Metadata filtering must be implemented against the JSON metadata column using Oracle JSON functionality.

## Serialization

The implementation must use LangGraph's serializer protocol.

Requirements:

- Use the serializer provided to the saver, defaulting to LangGraph's default serializer behavior.
- Store channel values and writes as typed serialized blobs.
- Store checkpoint and metadata as JSON-compatible data.
- Preserve type tags returned by LangGraph serialization.
- Avoid custom serialization formats unless the spec is updated first.

## Transaction Behavior

Checkpoint operations must be atomic.

Requirements:

- `put` must persist checkpoint rows and associated blob rows in a single transaction.
- `put_writes` must persist all writes for a call in a single transaction.
- Delete, copy, and prune operations must be atomic.
- The implementation must not commit caller-owned connections unless the saver explicitly owns the connection or the API contract documents that behavior.
- Rollback behavior must be explicit for failures during writes or migrations.

## Concurrency and Idempotency

The implementation must support concurrent LangGraph runs for different `thread_id` values.

Requirements:

- Repeated `put` calls for the same `(thread_id, checkpoint_ns, checkpoint_id)` must be idempotent and update checkpoint and metadata consistently.
- Repeated `put_writes` calls for duplicate write keys must follow LangGraph semantics:
  - special write indexes are updated when LangGraph expects replacement;
  - regular duplicate writes should not corrupt existing writes.
- Operations must use bind variables, never string interpolation for user-provided values.
- Table names and prefixes must be validated before interpolation into SQL.

## Delta Channel Support

The checkpointer must preserve enough parent-chain and write history for LangGraph delta channel reconstruction.

The first implementation may rely on LangGraph's default `get_delta_channel_history` behavior if `get_tuple` and `parent_config` are correct. A storage-optimized override may be added only after the baseline behavior is tested.

`prune(strategy="keep_latest")` must not break delta channel reconstruction. If safe pruning cannot be implemented in the first version, `keep_latest` must raise a documented exception instead of silently deleting required ancestor state.

## Error Handling

The implementation must define project-specific exceptions for:

- invalid configuration
- missing required LangGraph config values
- unsupported connection object
- setup or migration failure
- database operation failure

Exceptions must preserve the original `oracledb` exception as context.

Sensitive values such as passwords, wallet passwords, and connection strings must not appear in exception messages.

## Testing Requirements

Tests must be written with `pytest` and executed in the `langgraph-oracle` Conda environment.

Unit tests must cover:

- configuration validation
- SQL identifier validation for schema prefixes
- migration ordering
- serialization and deserialization helpers
- checkpoint tuple assembly
- write index handling
- error wrapping
- public API import behavior

Integration tests must be marked with:

```python
@pytest.mark.integration
@pytest.mark.oracle_adb
```

Integration tests must be skipped unless all required ADB connection settings are provided through environment variables.

Expected integration test environment variables:

- `LANGGRAPH_ORACLE_ADB_USER`
- `LANGGRAPH_ORACLE_ADB_PASSWORD`
- `LANGGRAPH_ORACLE_ADB_DSN`
- `LANGGRAPH_ORACLE_ADB_CONFIG_DIR`
- `LANGGRAPH_ORACLE_ADB_WALLET_LOCATION`
- `LANGGRAPH_ORACLE_ADB_WALLET_PASSWORD`

Integration tests must cover:

- `setup`
- `put`
- `get_tuple`
- `list`
- `put_writes`
- `delete_thread`
- basic LangGraph graph execution with the Oracle ADB checkpointer

## Acceptance Criteria

The first implementation is complete when:

- The saver conforms to LangGraph's `BaseCheckpointSaver` contract.
- Only `oracledb` is used for Oracle Database access.
- The schema is created and migrated by explicit setup.
- Unit tests run with `pytest` in the `langgraph-oracle` Conda environment.
- Integration tests are documented, marked, and skipped unless ADB settings are present.
- Formatting with `black` passes.
- Linting with `pylint` passes.
- Documentation includes a minimal LangGraph usage example.
- `CHANGELOG.md` records the implementation under the current date.

## References

- LangGraph checkpoint base API: https://github.com/langchain-ai/langgraph/blob/main/libs/checkpoint/langgraph/checkpoint/base/__init__.py
- LangGraph Postgres checkpointer implementation: https://github.com/langchain-ai/langgraph/tree/main/libs/checkpoint-postgres
- python-oracledb documentation: https://python-oracledb.readthedocs.io/
