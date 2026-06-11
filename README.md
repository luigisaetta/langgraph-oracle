# langgraph-oracle

![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)
![Black](https://img.shields.io/badge/code%20style-black-000000)
![Pylint](https://img.shields.io/badge/lint-pylint-yellowgreen)
![Pytest](https://img.shields.io/badge/tests-pytest-blueviolet)

`langgraph-oracle` provides reusable facilities that make it easier to build and operate LangGraph applications in Oracle Cloud Infrastructure (OCI) environments.

The first facility planned for this repository is a robust LangGraph checkpointer backed by Oracle Autonomous Database (ADB). The project will then grow with additional OCI-oriented helpers and integrations that keep LangGraph applications easier to configure, persist, test, and run in production.

## Goals

- Provide OCI-native facilities for LangGraph applications.
- Implement a reliable Oracle ADB-backed LangGraph checkpointer.
- Keep the public API aligned with LangGraph conventions.
- Make configuration, connection handling, transactions, and error behavior explicit.
- Support local development and CI without requiring live OCI resources for every test.
- Separate unit tests from integration tests that require Oracle ADB or OCI credentials.
- Document each facility before implementation.

## Initial Scope

The initial implementation will focus on the Oracle ADB checkpointer.

Expected capabilities include:

- Store, retrieve, list, and update LangGraph checkpoints using Oracle ADB.
- Define and document the database schema and migration approach.
- Support safe serialization and compatibility expectations.
- Provide clear configuration for database connectivity and authentication.
- Handle connection lifecycle, transaction boundaries, and operational errors predictably.
- Include examples showing usage in a minimal LangGraph application.

Additional facilities may be added later, but they should follow the same principles: small, explicit adapters; OCI-aware behavior; readable code; and strong tests.

## Development Workflow

This repository follows a spec-driven workflow.

1. Write or update the relevant specification under `specs/`.
2. Review the specification for scope, behavior, acceptance criteria, and tests.
3. Implement code according to the specification.
4. Add or update unit tests.
5. Run formatting, linting, and tests.
6. Record significant changes in `CHANGELOG.md`.

Code should not be added before the related specification exists.

## Local Setup

Create the required Conda environment:

```bash
conda env create -f environment.yml
```

Activate it:

```bash
conda activate langgraph-oracle
```

The environment installs the package in editable mode with development dependencies.

## Testing Environment

All tests must be executed in the `langgraph-oracle` Conda environment.

Use:

```bash
conda run -n langgraph-oracle python -m pytest
```

Formatting and linting:

```bash
conda run -n langgraph-oracle python -m black --check src tests
conda run -n langgraph-oracle python -m pylint src tests
```

The expected toolchain is:

- `black` for Python formatting.
- `pylint` for Python linting.
- `pytest` for tests.

Unit tests should not require live OCI resources. Integration tests that require Oracle ADB or OCI credentials must be clearly marked and documented.

## Concurrency Notes

`OracleADBCheckpointer` is designed for concurrent LangGraph runs, with the following current behavior:

- Operations touching the same `thread_id` are serialized inside one Python process.
- A single `oracledb.Connection` is protected by a connection lock.
- An `oracledb.ConnectionPool` is allowed to provide a separate connection per operation, so different `thread_id` values can proceed concurrently.
- Regular duplicate pending writes are treated as idempotent when Oracle reports a unique constraint race.

Administrative operations such as `delete_thread`, `delete_for_runs`, and `copy_thread` should not be run against an active thread from another process unless the caller coordinates that lifecycle externally.

## Repository Status

The repository is currently at the project bootstrap stage.

Current artifacts:

- `AGENTS.md`: project guidance for agents and contributors.
- `README.md`: project overview and development expectations.
- `LICENSE`: MIT license.
- `specs/oracle_adb_checkpointer.md`: first feature specification.
- `pyproject.toml`: Python packaging, formatting, linting, and pytest configuration.
- `environment.yml`: Conda environment definition for `langgraph-oracle`.
- `src/langgraph_oracle/`: package skeleton.
- `tests/`: pytest test suite.
- `CHANGELOG.md`: dated record of significant project changes.

## Design Principles

- Prefer readable, maintainable code over clever or dense abstractions.
- Follow LangGraph public interfaces and naming conventions wherever possible.
- Keep Oracle and OCI-specific behavior isolated behind explicit adapters.
- Never hard-code credentials, tenancy details, wallet paths, or connection strings.
- Keep database schema changes explicit, versioned, and documented.
- Make error handling direct and predictable.
- Write tests that explain behavior as executable documentation.

## License

This project is licensed under the MIT License.
