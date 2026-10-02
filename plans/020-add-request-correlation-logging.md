# Plan 020: Add request correlation and structured service logging

> **Executor instructions**: Use the existing Pino dependency and preserve CLI human output. Never log tokens, authorization headers, RPC credentials or raw transaction hex.
>
> **Drift check (run first)**: `git diff --stat c90e552..HEAD -- apps/api/src/index.ts apps/onlydoge/src/index.ts packages/platform/src/logger.ts packages/platform/src packages/modules tests`

## Status
- **Priority**: P2
- **Effort**: M
- **Risk**: LOW
- **Depends on**: `plans/001-restore-verification-baseline.md`
- **Category**: DX/observability
- **Planned at**: commit `c90e552`, 2026-07-15

## Why this matters
Request IDs exist only when audit input is built; they are not returned to clients or attached to error logs. A Pino factory exists but runtime services use `console`, making request and background failures hard to correlate.

## Current state
- `packages/platform/src/logger.ts:1-7` exports an unused Pino factory.
- `apps/api/src/index.ts:360-421` logs API errors with `console` and no request ID.
- `apps/api/src/index.ts:440-477` generates/reads request ID only while building audit input.
- `packages/platform/src/mempool-appear-detector.ts:124-143` uses string-interpolated console errors.

## Target conventions
- Accept `x-request-id` only when trimmed, bounded and syntactically safe; otherwise generate UUID.
- One ID per request, returned in `x-request-id`, stored in audit and present on all request logs/errors.
- Pino child loggers carry stable fields (`service`, `component`, `requestId`, chain/instance when relevant).
- Errors use structured `err`/`cause`; messages remain credential-masked.

## Scope
**In scope**
- Logger configuration/port and runtime injection.
- API request lifecycle/error/audit correlation.
- Replace runtime service `console.*` call sites; scripts may retain console output.
- Logging tests/docs.
- `plans/README.md` (status only)

**Out of scope**
- OpenTelemetry/vendor backend.
- Logging request/response bodies.
- Changing API error response text except response header.

## Steps
1. Harden `createLogger` with explicit redaction paths and export a minimal logger type usable by modules without importing global state.
2. Add an API lifecycle hook/context that resolves the request ID once before handlers, sets the response header, and creates a child logger. Pass the same ID into `auditEventInput`; remove its independent generation.
3. Update infrastructure/not-found/unhandled handlers to emit structured logs with route, code, status and request ID. Known client/domain errors should not be logged as server errors unless policy says so.
4. Inject component child loggers into indexer, mempool detector/sampler, ZMQ and warehouse paths. Replace string-interpolated console calls while preserving useful fields.
5. Add tests for generated/preserved/rejected IDs, response header equals audit row, error log correlation, redaction and background component fields.
6. Document log format and correlation workflow in `docs/production-runbook.md`.

## Verification
- API integration tests and logger unit tests pass.
- `rg "console\\.(log|info|warn|error)" apps packages` returns only approved CLI/bootstrap exceptions.
- A test logger destination contains no token/credential fixture.
- `bun run ci` exits 0.

## Done criteria
- [ ] Every API response has one request ID.
- [ ] Audit and logs use the same ID.
- [ ] Runtime services log structured component fields.
- [ ] Sensitive fields are redacted/tested.

## STOP conditions
- Elysia lifecycle cannot expose one context to both response and global error hooks; add a documented request-scoped store rather than regenerating.
- Pino serialization reveals nested credentials not covered by redaction; fix redaction before rollout.

## Maintenance notes
New background services must receive a child logger; do not import/create independent global loggers inside domain/application modules.
