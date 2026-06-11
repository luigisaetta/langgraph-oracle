# Oracle ADB Checkpointer

This guide explains how to use `OracleADBCheckpointer` in a LangGraph agent.

The checkpointer stores LangGraph checkpoints in Oracle Autonomous Database (ADB) using only the official `oracledb` Python package.

## What It Does

LangGraph checkpointers let an agent persist graph state between invocations.

With this checkpointer, a LangGraph application can:

- resume a thread by `thread_id`;
- persist checkpoints in Oracle ADB;
- store pending writes produced during graph execution;
- keep checkpoint data outside the application process;
- use an `oracledb.Connection` or `oracledb.ConnectionPool`.

## Prerequisites

You need:

- Python 3.11 or later.
- A LangGraph application.
- Oracle Autonomous Database.
- A dedicated database schema owner for LangGraph checkpointing.
- A dedicated schema owner allowed to create tables and indexes during the first setup.
- The ADB wallet downloaded and unzipped locally, unless your environment uses a different supported `oracledb` connection mode.
- The `langgraph-oracle` package installed in your Python environment.

For local development in this repository, use the Conda environment:

```bash
conda env create -f environment.yml
conda activate langgraph-oracle
```

## Install In Another Project

Until the package is published, install it from a local checkout:

```bash
pip install -e /path/to/langgraph-oracle
```

Your application also needs LangGraph and python-oracledb. They are declared as runtime dependencies by this package:

```bash
pip install langgraph oracledb
```

## Recommended Environment Variables

Do not hard-code credentials or wallet paths.

Use environment variables or your application's secret manager:

```bash
export LANGGRAPH_ORACLE_ADB_USER="LANGGRAPH_CHECKPOINT"
export LANGGRAPH_ORACLE_ADB_PASSWORD="..."
export LANGGRAPH_ORACLE_ADB_DSN="myadb_low"
export LANGGRAPH_ORACLE_ADB_CONFIG_DIR="/path/to/wallet"
export LANGGRAPH_ORACLE_ADB_WALLET_LOCATION="/path/to/wallet"
export LANGGRAPH_ORACLE_ADB_WALLET_PASSWORD="..."
```

For ADB wallet connections, `config_dir` and `wallet_location` usually point to the unzipped wallet directory.

## Dedicated Schema

Do not use the ADB administrative schema for checkpointing.

Create and use a dedicated schema owner for LangGraph checkpoint tables, for example `LANGGRAPH_CHECKPOINT`. The application should connect as that schema owner, and `setup()` should create the checkpoint tables inside that schema.

This keeps checkpoint data isolated from administrative objects and makes permissions, cleanup, backup, and auditing easier to manage.

The exact SQL for creating the schema user depends on your organization's security standards. At minimum, the dedicated schema needs enough privileges to connect and to create the checkpointer tables and indexes during setup. After setup, runtime usage only needs privileges on those checkpointer objects.

## One-Time Database Setup

Run `setup()` once before using the checkpointer.

Connect as the dedicated checkpoint schema owner, then call `setup()`.

`setup()` creates the checkpointer tables and indexes in the connected schema. It does not run silently during normal graph execution.

```python
import os

from langgraph_oracle import OracleADBCheckpointer


with OracleADBCheckpointer.from_connection_params(
    user=os.environ["LANGGRAPH_ORACLE_ADB_USER"],
    password=os.environ["LANGGRAPH_ORACLE_ADB_PASSWORD"],
    dsn=os.environ["LANGGRAPH_ORACLE_ADB_DSN"],
    config_dir=os.environ["LANGGRAPH_ORACLE_ADB_CONFIG_DIR"],
    wallet_location=os.environ["LANGGRAPH_ORACLE_ADB_WALLET_LOCATION"],
    wallet_password=os.environ["LANGGRAPH_ORACLE_ADB_WALLET_PASSWORD"],
) as checkpointer:
    checkpointer.setup()
```

By default, tables use the `LG` prefix:

- `LG_CHECKPOINT_MIGRATIONS`
- `LG_CHECKPOINTS`
- `LG_CHECKPOINT_BLOBS`
- `LG_CHECKPOINT_WRITES`

You can choose a different prefix:

```python
OracleADBCheckpointer.from_connection_params(
    table_prefix="MYAPP",
    ...
)
```

Use only letters, digits, and underscores, starting with a letter.

## Minimal LangGraph Usage

This example shows the checkpointer attached to a small LangGraph graph.

```python
import os
from typing import TypedDict

from langgraph.graph import StateGraph
from langgraph_oracle import OracleADBCheckpointer


class AgentState(TypedDict):
    message: str
    count: int


def respond(state: AgentState) -> AgentState:
    return {
        "message": f"Received: {state['message']}",
        "count": state["count"] + 1,
    }


builder = StateGraph(AgentState)
builder.add_node("respond", respond)
builder.set_entry_point("respond")
builder.set_finish_point("respond")

with OracleADBCheckpointer.from_connection_params(
    user=os.environ["LANGGRAPH_ORACLE_ADB_USER"],
    password=os.environ["LANGGRAPH_ORACLE_ADB_PASSWORD"],
    dsn=os.environ["LANGGRAPH_ORACLE_ADB_DSN"],
    config_dir=os.environ["LANGGRAPH_ORACLE_ADB_CONFIG_DIR"],
    wallet_location=os.environ["LANGGRAPH_ORACLE_ADB_WALLET_LOCATION"],
    wallet_password=os.environ["LANGGRAPH_ORACLE_ADB_WALLET_PASSWORD"],
) as checkpointer:
    graph = builder.compile(checkpointer=checkpointer)

    result = graph.invoke(
        {"message": "hello", "count": 0},
        config={"configurable": {"thread_id": "user-123"}},
    )

print(result)
```

The `thread_id` is the logical conversation or workflow id. Reusing the same `thread_id` lets LangGraph resume from previously persisted checkpoints.

## Using A Connection Pool

For applications serving multiple users or requests, prefer an `oracledb.ConnectionPool`.

```python
import os

import oracledb
from langgraph_oracle import OracleADBCheckpointer


pool = oracledb.create_pool(
    user=os.environ["LANGGRAPH_ORACLE_ADB_USER"],
    password=os.environ["LANGGRAPH_ORACLE_ADB_PASSWORD"],
    dsn=os.environ["LANGGRAPH_ORACLE_ADB_DSN"],
    config_dir=os.environ["LANGGRAPH_ORACLE_ADB_CONFIG_DIR"],
    wallet_location=os.environ["LANGGRAPH_ORACLE_ADB_WALLET_LOCATION"],
    wallet_password=os.environ["LANGGRAPH_ORACLE_ADB_WALLET_PASSWORD"],
    min=1,
    max=8,
    increment=1,
)

checkpointer = OracleADBCheckpointer(pool)
graph = builder.compile(checkpointer=checkpointer)
```

With a pool, operations for different `thread_id` values can use different database connections.

## Concurrency Notes

The synchronous checkpointer is safe for typical multi-threaded application use in one Python process:

- operations for the same `thread_id` are serialized inside the process;
- a single `oracledb.Connection` is protected by a connection lock;
- an `oracledb.ConnectionPool` can serve concurrent operations for different threads;
- duplicate regular pending writes caused by races are treated as idempotent.

The current locks are process-local. If multiple application processes or containers use the same ADB, they do not share Python locks.

Do not run administrative operations such as `delete_thread`, `delete_for_runs`, or `copy_thread` on a thread that is actively being used by another process unless your application coordinates that lifecycle externally.

## Async Applications

`OracleADBCheckpointer` is synchronous.

The async LangGraph saver methods intentionally raise an explicit unsupported-operation error instead of wrapping blocking calls. A future async saver should use `oracledb.AsyncConnection` or `oracledb.AsyncConnectionPool` directly.

## Testing ADB Connectivity

Before wiring the checkpointer into an agent, test that `oracledb` can connect to your ADB:

```python
import os

import oracledb


with oracledb.connect(
    user=os.environ["LANGGRAPH_ORACLE_ADB_USER"],
    password=os.environ["LANGGRAPH_ORACLE_ADB_PASSWORD"],
    dsn=os.environ["LANGGRAPH_ORACLE_ADB_DSN"],
    config_dir=os.environ["LANGGRAPH_ORACLE_ADB_CONFIG_DIR"],
    wallet_location=os.environ["LANGGRAPH_ORACLE_ADB_WALLET_LOCATION"],
    wallet_password=os.environ["LANGGRAPH_ORACLE_ADB_WALLET_PASSWORD"],
) as connection:
    with connection.cursor() as cursor:
        cursor.execute("SELECT 1 FROM dual")
        print(cursor.fetchone())
```

If this fails, fix the ADB wallet, network access, credentials, or DSN before using the LangGraph checkpointer.

## Troubleshooting

`LangGraph config must include configurable.thread_id.`

Pass a thread id when invoking the graph:

```python
config={"configurable": {"thread_id": "user-123"}}
```

`Oracle ADB operation failed.`

Inspect the original exception chained to the error. Common causes include missing tables, invalid wallet configuration, expired credentials, or insufficient privileges.

`keep_latest pruning is not implemented`

This is intentional. Naive pruning can break LangGraph `DeltaChannel` reconstruction. Use `prune(strategy="delete")` only when you want to remove all state for a thread.
