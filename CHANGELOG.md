# Changelog

## 2026-06-11

- Added the Oracle ADB LangGraph checkpointer specification.
- Initialized Python packaging and pytest configuration.
- Added the `langgraph-oracle` Conda environment definition.
- Added initial bootstrap tests for project structure.
- Added the first synchronous Oracle ADB checkpointer implementation.
- Added unit tests for schema validation, serialization, and checkpointer behavior.
- Improved checkpointer concurrency behavior with per-thread locks, pool-aware connection handling, and duplicate-write race handling.
- Documented concurrency expectations and administrative-operation guardrails.
- Refactored Oracle checkpointer SQL statement generation into a dedicated module to keep the main checkpointer implementation easier to maintain.
- Added a dedicated Oracle ADB checkpointer usage guide for LangGraph agents.
- Updated Oracle ADB documentation and examples to require a dedicated checkpoint schema instead of the ADB administrative schema.
