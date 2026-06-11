# langgraph-oracle

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

## Testing Environment

All tests must be executed in the `langgraph-oracle` Conda environment.

Typical commands will be documented once the package structure is created. The expected toolchain is:

- `black` for Python formatting.
- `pylint` for Python linting.
- `pytest` for tests.

Unit tests should not require live OCI resources. Integration tests that require Oracle ADB or OCI credentials must be clearly marked and documented.

## Repository Status

The repository is currently at the project bootstrap stage.

Current artifacts:

- `AGENTS.md`: project guidance for agents and contributors.
- `README.md`: project overview and development expectations.
- `LICENSE`: MIT license.

Planned next artifacts:

- `specs/`: specifications for the Oracle ADB checkpointer and future facilities.
- `CHANGELOG.md`: dated record of significant project changes.
- Python package structure for the implementation.
- Unit and integration test structure.

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
