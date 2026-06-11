# AGENTS.md

This project provides a set of facilities that make it easier to use LangGraph in Oracle Cloud Infrastructure (OCI) environments.

The first major facility is a robust LangGraph checkpointer backed by Oracle Autonomous Database (ADB). Future facilities should follow the same design principles: OCI-native integration, clear operational behavior, strong test coverage, and compatibility with LangGraph conventions.

## Project Guidelines

- All documentation and Markdown files must be written in English.
- Specifications must be written before implementation and stored under the `specs/` directory.
- Code must be generated only after the relevant specification exists.
- Implemented code must conform to the approved specification.
- Python code must be formatted with `black`.
- Python code must be checked with `pylint`.
- New functionality must include unit tests written with `pytest`.
- Unit tests must provide sufficient coverage, with a target above 80%.
- All tests must be executed in the `langgraph-oracle` Conda environment.
- Significant features, fixes, refactorings, specification updates, packaging changes, and documentation updates must be recorded in `CHANGELOG.md` under the current date.
- Done means: specification written or updated, code formatted, tests written, lint checks completed, tests executed, and all test and lint issues resolved.

## Project Scope

The repository is intended to host reusable Python components for LangGraph applications running in or integrating with OCI.

Initial scope:

- A LangGraph checkpointer implementation for Oracle ADB.
- Connection and configuration helpers suitable for OCI deployments.
- Clear examples showing how to wire the facilities into LangGraph applications.
- Tests that cover persistence behavior, error handling, and compatibility with expected LangGraph checkpointer semantics.

Out of scope unless explicitly specified:

- Application-specific agent logic.
- UI components.
- Deployment automation unrelated to the library facilities.
- OCI resources that cannot be tested or documented repeatably.

## Python Code Conventions

Every Python source file must start with a multiline header using this format:

```python
"""
Author: L. Saetta
Date last modified: YYYY-MM-DD
License: MIT
Description: Brief description of the responsibilities and functions contained in this file.
"""
```

Use the actual modification date when creating or updating a Python source file.

All generated Python code must include accurate docstrings for modules, classes, methods, and functions where applicable. Docstrings must follow the Google Python docstring format and clearly describe purpose, arguments, return values, raised exceptions, and relevant behavior.

## Human Readability and Maintainability

Code generated for this repository must be optimized for human readability first.

Generated code must be easy to read, review, test, and maintain by a human engineer. Prefer clear structure and explicit intent over cleverness, dense abstractions, or overly compact expressions.

Follow these principles:

- Use descriptive names for modules, classes, functions, methods, variables, and tests.
- Keep functions focused on one clear responsibility.
- Prefer straightforward control flow over deeply nested logic.
- Extract helpers only when they reduce real complexity or meaningful duplication.
- Keep public behavior easy to trace from LangGraph inputs to persisted state and restored checkpoints.
- Make error handling explicit and predictable.
- Avoid hidden side effects and implicit global state.
- Keep configuration access centralized and easy to audit.
- Keep database schema changes explicit, versioned, and documented.
- Use comments sparingly, only when they clarify non-obvious decisions or complex logic.
- Preserve consistency with the existing code style and project structure.
- Write tests that describe behavior clearly and can be understood as executable documentation.

Readable code is part of the quality bar for this project. A change is not considered complete if it works technically but is unnecessarily difficult to understand or maintain.

## LangGraph and OCI Design Principles

- Follow LangGraph public interfaces and naming conventions wherever possible.
- Keep the checkpointer behavior compatible with LangGraph expectations for checkpoint storage, retrieval, listing, and writes.
- Prefer small, explicit adapters around OCI and Oracle Database APIs instead of leaking vendor-specific details throughout the codebase.
- Make local development and CI practical without requiring live OCI resources for every test.
- Separate pure unit tests from integration tests that require Oracle ADB or OCI credentials.
- Never hard-code credentials, tenancy details, wallet paths, or connection strings.
- Treat connection lifecycle, retries, transaction boundaries, and cleanup as part of the public reliability contract.

## Spec-Driven Development Workflow

1. Write or update the specification in `specs/`.
2. Review the specification for scope, behavior, acceptance criteria, and test expectations.
3. Implement the code according to the specification.
4. Add or update unit tests.
5. Run formatting, linting, and tests.
6. Fix all issues before considering the work done.

## Checkpointer Expectations

The Oracle ADB checkpointer specification should define:

- The LangGraph interfaces and versions it targets.
- The database schema and migration strategy.
- Required configuration values and supported authentication modes.
- Serialization format and compatibility expectations.
- Transaction behavior and concurrency assumptions.
- Error handling for connection failures, invalid configuration, and database constraint violations.
- Unit test strategy and optional integration test strategy.
- Example usage in a minimal LangGraph application.
