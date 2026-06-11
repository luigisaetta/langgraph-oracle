# langgraph-oracle

`langgraph-oracle` provides reusable facilities that make it easier to build and operate LangGraph applications in Oracle Cloud Infrastructure (OCI) environments.

The first feature is an Oracle Autonomous Database (ADB) checkpointer for LangGraph.

## Start Here

- [Oracle ADB Checkpointer](oracle-adb-checkpointer.md): configure and use the checkpointer in LangGraph agents.
- [Changelog](changelog.md): track notable project changes.

## Current Focus

The current implementation focuses on:

- persistent LangGraph checkpointing in Oracle ADB;
- direct database access through the official `oracledb` Python package;
- explicit schema setup;
- readable and maintainable Python code;
- pytest-based validation in the `langgraph-oracle` Conda environment.

## Repository

Source code is available on GitHub:

[luigisaetta/langgraph-oracle](https://github.com/luigisaetta/langgraph-oracle)
