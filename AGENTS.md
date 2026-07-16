# AGENTS.md

This file provides guidance to agents working in this repository.

## Purpose and Precedence

- MUST means required.
- SHOULD means recommended unless there is a concrete reason to deviate.
- MAY means optional.
- Root `AGENTS.md` defines repository-wide defaults. If a deeper path later adds a more specific `AGENTS.md`, the deeper file SHOULD be treated as higher precedence for that subtree.

## Non-negotiables

1. Correctness first. TiDB is a distributed SQL database; seemingly small changes can alter SQL semantics, consistency, or cluster behavior.
2. No speculative behavior. Do not invent APIs, defaults, protocol behavior, or test workflows.
3. Keep diffs minimal. Avoid unrelated refactors, broad renames, or formatting-only churn unless explicitly requested.
4. Leave verifiable evidence. Run targeted checks and report exact commands.
5. Respect generated code artifacts. Do not hand-edit generated code outputs; regenerate from source inputs.

## Quick Decision Matrix

| Task | Required action |
| --- | --- |
| Added/moved/renamed/removed Go files, changed Bazel files, updated Bazel test targets, or changed `go.mod`/`go.sum` | MUST run `make bazel_prepare` and include resulting Bazel metadata changes in the PR (for example `BUILD.bazel`, `**/*.bazel`, and `**/*.bzl`). |
| Running package unit tests | SHOULD run targeted tests (`go test -run <TestName> -tags=intest,deadlock`) and avoid full-package runs unless needed. |
| Unit tests in a package that uses failpoints | MUST enable failpoints before tests and disable afterward (see `docs/agents/testing-flow.md`). |
| Recording integration tests | MUST use `pushd tests/integrationtest && ./run-tests.sh -r <TestName> && popd` (not `-record`; `-record` is for unit-test suites that explicitly support it). |
| RealTiKV tests | MUST start playground in background, run tests, then clean up playground/data (see `docs/agents/testing-flow.md`). |
| Bug fix | MUST add a regression test and verify it fails before fix and passes after fix. |
| Fmt-only PR | MUST NOT run costly `realtikvtest`; local compilation is enough. |
 Before finishing | MUST run `make lint` if there are code changes. SHOULD self-review diff quality before finishing. |

### Skills

- Repository-level Codex skills are maintained under `.agents/skills` (relative to the repository root / current working directory).
- Keep skill content and references together under each skill folder (for example: `.agents/skills/<skill>/SKILL.md` and `.agents/skills/<skill>/references/`).
- `.github/skills` is kept only as a migration note path and should not be used as the primary location for new skill updates.

## Pre-flight Checklist

1. Restate the task goal and acceptance criteria.
2. Locate the owning subsystem and the closest existing tests (`Repository Map`, `Task -> Validation Matrix`). If the target package has `doc.go`, agents MUST read that package-level doc first before diving into implementation files.
3. Decide prerequisites before running tests/build (`docs/agents/testing-flow.md` -> `Failpoint decision for unit tests`; `AGENTS.md` -> `Build Flow` -> `When make bazel_prepare is required`).
4. Pick the smallest valid validation set and prepare final reporting items (`Agent Output Contract`).
5. If `AGENTS.md` or docs under `docs/agents/` changed, follow the checklist in `docs/agents/agents-review-guide.md` before finishing.

See `docs/agents/architecture-index.md`.

## Building

```
make build
```

## Testing

See `docs/agents/test-guide.md`.
- If you modify `pkg/parser/parser.y`/`parser.go`, run `make fmt` from `pkg/parser` before commit (then regenerate with `make parser`).

Use the smallest set that still proves correctness.
Command details for package, integration-test, and RealTiKV surfaces live in `docs/agents/testing-flow.md`.

| Change scope | Minimum validation |
| --- | --- |
| `pkg/planner/**` rules or logical/physical plans | Targeted planner unit tests and update rule testdata when needed |
| `pkg/executor/**` SQL behavior | Targeted unit test plus relevant integration test (`tests/integrationtest`) |
| `pkg/expression/**` builtins or type inference | Targeted expression unit tests with edge-case coverage |
| `pkg/session/**` / variables / protocol behavior | Targeted package tests plus SQL integration tests for user-visible behavior |
| `pkg/ddl/**` schema changes | DDL-focused unit/integration tests and compatibility impact checks |
| `pkg/store/**` / `pkg/kv/**` storage behavior | Targeted unit tests; use realtikv tests if behavior depends on real TiKV |
| Parser files (`pkg/parser/**`) | Parser-specific Make targets (`make parser`, `make parser_yacc`, `make parser_fmt`, `make parser_unit_test`) and related unit tests |
| `tests/integrationtest/t/**` changed | Record and verify regenerated result correctness (see `docs/agents/testing-flow.md` -> `Integration tests`) |
| `tests/realtikvtest/**` changed | Start playground, run scoped tests, then mandatory cleanup (see `docs/agents/testing-flow.md` -> `RealTiKV tests`) |

This project requires extremely high code quality and maintainability. Best engineering practices must be followed at all times.

- Detailed command playbooks live in `docs/agents/testing-flow.md`.
- Select required test surfaces first (`Task -> Validation Matrix`), then run scoped commands from the playbook.
- Use `.agents/skills/tidb-verify-profile` to pick a validation profile (`WIP` / `Ready` / `Heavy`). `Ready` is required before any final-status claim; trigger phrases are defined in `Quick Decision Matrix`.
- All other testing rules (failpoints, integration recording, RealTiKV lifecycle, regression tests) are stated once in `Quick Decision Matrix` above; do not duplicate them here.

### Core Principles: Simplicity & Readability

- Boring Code - Obvious, self-explanatory > clever, minimize cognitive load
- Single Responsibility - One function, one job
- Explicit over Implicit - Clear is better than concise
- Meaningful Abstractions - Only when they reduce cognitive load
- Keep DRY - Only if it does not conflict with the above principles

### Better Maintainability

- Keep the public API surface minimal; prefer reusing existing methods
- Don't treat "looks similar" as "equivalent"
- Abstractions must be meaningful
- Keep certainty, single source of truth - e.g. don't introduce "optional" unless absolutely necessary
- Always add clear, concise and explicit doc comments for public APIs, complex logic, and non-obvious code

For non-refactor tasks:

- Prefer minimal, necessary, localized changes
- Follow existing code patterns, even if they are not ideal or party against our principles
  - Do your best to trade off between "ideal code", "consistent codebase", "only change what is necessary"
  - Point out any refactor chance you discovered after finishing your work, avoid changing them in the same work. Let human decides later.

### Naming Symbols

- Choosing the name that needs the least explanation, consider: verb clarity, noun specificity, context
- Name tests by behavior and expectation.

### Conflict Management

To minimize conflicts that arise when merging commits that are ahead of upstream TiDB, reduce code maintenance costs, and alleviate the mental burden of resolving conflicts, the following guidelines should be followed when developing PingKaiDB-specific features:
- Minimize intrusions into the TiDB codebase. For existing TiDB code files, make as few modifications as possible and insert only necessary entry functions.
- The main logic should be written in separate code files and named with the prefix `pkdb_`.
- If they exist as a separate package, the package name should begin with `pkdb`, such as `pkdblicense`.

## Pull Request Guide

- See `docs/agents/pull-request-guide.md`.

## Agent Output Contract

When finishing a coding task, report:

1. Files changed.
2. Validation profile used (`WIP`, `Ready`, or `Heavy`) and why.
3. Risks: correctness, compatibility, performance.
4. Exact commands run for validation.
5. What was not verified locally.
