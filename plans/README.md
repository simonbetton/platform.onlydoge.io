# OnlyDoge deep-improvement plans

These 26 implementation plans cover all 25 findings selected from the deep audit. Finding F05 is split into two plans because database CAS atomicity must land before continuous lease ownership. Plans were written against commit `c90e552` on 2026-07-15 and assume the pre-existing dirty working tree is preserved.

## Executor protocol

1. Select the first dependency-ready `TODO`.
2. Run its drift check before editing.
3. Change only files listed in that plan's scope.
4. Run every targeted gate and `bun run ci`.
5. Update only that row to `DONE`, `BLOCKED`, or `STALE`; include a short reason/link in the Notes column.
6. Stop rather than improvising when a plan's STOP condition applies.

Statuses: `TODO`, `IN PROGRESS`, `BLOCKED`, `DONE`, `STALE`.

## Recommended execution waves

1. **Safety baseline**: 001 and 002.
2. **Independent bounded fixes**: 003, 005, 009–011, 013–014, 021–024.
3. **Concurrency foundations**: 007, then 006; 004 may run independently after 001.
4. **Production proof**: 015 after 024.
5. **High-risk persistence work**: 018 and 019 after 015; 008 after 006/007/015; 016 after 015/018.
6. **API and observability work**: 012, 017 and 020 when their direct dependencies are complete.
7. **Cleanup and documentation**: 025 after 019; 026 last.

Within a wave, dependency-independent plans may run in parallel in isolated branches/worktrees.

## Dependency graph

```text
001 ─┬─ 003,004,005,009..011,013,014,020..024
     │
     ├─ 007 ─┬─ 006 ─────────────┐
     │       └─ 012              │
     │                            ├─ 008
024 ─┴─ 015 ─┬─ 018 ─┬─ 016      │
             │       └────────────┘
             └─ 019 ─── 025

009 ─ 017
003 + 012 + 017 + 018 ─ 026
002 has no dependency.
```

## Status index

| Plan | Audit finding | Priority | Depends on | Status | Notes |
|---|---|---:|---|---|---|
| [001](001-restore-verification-baseline.md) | F01 verification baseline | P1 | — | DONE | Baseline lint, typecheck, tests, CI, and diff checks pass |
| [002](002-remove-workspace-tls-key.md) | F02 workspace TLS material | P1 | — | DONE | Relocated outside workspace; ignore/history/CI checks pass |
| [003](003-wire-production-dogecoin-runtime.md) | F03 production RPC/ZMQ | P1 | 001 | DONE | Required RPC and optional indexer ZMQ wiring tested |
| [004](004-make-admin-bootstrap-atomic.md) | F04 admin bootstrap race | P1 | 001 | DONE | Atomic all-driver bootstrap; concurrency passed 10/10 |
| [005](005-require-managed-analytics-credentials.md) | F06 analytics least privilege | P1 | 001 | DONE | Dedicated credential pair required; fallback removed and tested |
| [006](006-keep-indexer-leadership-alive.md) | F05 continuous lease ownership | P1 | 001, 007 | DONE | Owner-checked heartbeat/loss/timer tests verified |
| [007](007-make-metadata-cas-atomic.md) | F05 atomic leadership CAS | P1 | 001 | DONE | Atomic affected-row CAS; contention passed 10/10 |
| [008](008-recover-partial-clickhouse-windows.md) | F07 crash-idempotent apply | P1 | 006, 007, 015 | DONE | Write-ahead markers, recovery, and ClickHouse failpoint tests verified |
| [009](009-cap-explorer-query-cost.md) | F08 pagination/scan caps | P1 | 001 | DONE | Shared caps and explorer-only ClickHouse budgets verified |
| [010](010-preserve-raw-storage-errors.md) | F09 raw-storage errors | P1 | 001 | DONE | Typed absence only; infrastructure/corruption causes preserved |
| [011](011-reject-malformed-block-transactions.md) | F10 strict block parsing | P1 | 001 | DONE | Fail-closed transaction validation and no-write regressions pass |
| [012](012-bound-mempool-watch-work.md) | F11 bounded watch work | P1 | 001, 007 | DONE | Shared bounded cache/topology/degradation policy verified |
| [013](013-eliminate-duplicate-address-summary.md) | F12 duplicate summary queries | P2 | 001 | DONE | ClickHouse direct wiring and one-call regression verified |
| [014](014-strengthen-indexer-health.md) | F13 online health/lag | P1 | 001 | DONE | Error, freshness, and online-lag policy covered by pure tests |
| [015](015-add-production-adapter-ci-lane.md) | F14 production adapter tests | P1 | 001, 024 | DONE | Four-adapter/split-role lane passed twice; cleanup verified |
| [016](016-index-transaction-references.md) | F15 transaction refs/summaries | P2 | 015, 018 | DONE | Indexed refs, fact-only address lists, and backfill verified |
| [017](017-paginate-block-transactions.md) | F16 block transaction pages | P2 | 009 | DONE | Page-before-enrichment contract and global indexes verified |
| [018](018-version-clickhouse-migrations.md) | F17 ClickHouse migrations | P1 | 015 | DONE | Version/checksum ledger and real recovery/concurrency tests pass |
| [019](019-version-metadata-migrations.md) | F18 metadata migrations | P1 | 015 | DONE | Immutable multi-driver runner and status/lock matrices pass |
| [020](020-add-request-correlation-logging.md) | F19 structured correlation | P2 | 001 | DONE | Request IDs, structured API logs, and redaction tests verified |
| [021](021-update-libsql-ws-chain.md) | F20 libsql/ws advisory | P2 | 001 | DONE | libsql/ws patched; frozen install, audit gate, CI pass |
| [022](022-align-bun-versions.md) | F21 Bun version drift | P2 | 001 | DONE | Bun 1.3.14 aligned; frozen install/image consistency pass |
| [023](023-govern-zmq-bridge-dependencies.md) | F22 ZMQ dependency governance | P2 | 001 | DONE | npm lock enforced/audited; Dependabot, CI smoke, image pass |
| [024](024-pin-service-images.md) | F23 floating service images | P2 | 001 | DONE | Official multi-arch digests pinned; service smokes pass |
| [025](025-remove-unused-drizzle-tooling.md) | F24 unused Drizzle | P3 | 019 | DONE | Unused packages/config removed; frozen install/Fallow pass |
| [026](026-align-operator-docs-and-env.md) | F25 docs/env drift | P3 | 003, 012, 017, 018 | DONE | README/ADR/env examples and consistency test aligned |

## Deferred direction options

These were product directions, not defects, and were not converted into implementation plans in the “all 25 findings” selection:

- Automate analytics readiness/backfill certification.
- Expose authenticated platform/indexer status.
- Productize rebuild certification.
- Curate address-flow analytics.

## Considered and rejected

- **Run native zeromq inside Bun**: rejected by ADR 0001 because the native binding crashes Bun; retain the out-of-process Node bridge.
- **Replace the watch path with webhooks, WebSockets or Redis**: rejected by ADR 0001 for the current one-shot use case and operational constraints.
- **Treat Postgres NOTIFY as a durable event log**: rejected; NOTIFY is fan-out, while registration ordering/catch-up owns race recovery.
- **Make the local source bind mount production-immutable**: rejected as a finding; it is an intentional developer workflow. Documentation must describe its actual non-watch restart behavior.

## Global verification

After Plan 001 is complete, every implementation should finish with:

```sh
bun run lint
bun run typecheck
bun run test
bun run ci
git diff --check
```

Plans involving production adapters additionally run `bun run test:adapters` after Plan 015 introduces it.
