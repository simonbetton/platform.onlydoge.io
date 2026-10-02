# Plan 001: Restore the verification baseline

> **Executor instructions**: Follow every step and verification gate. Stop on any STOP condition; do not widen scope. When complete, update this plan's row in `plans/README.md`.
>
> **Drift check (run first)**: `git diff --stat c90e552..HEAD -- tests/unit/rpc.test.ts tests/unit/mempool-appear-detector.test.ts`
> If either cited excerpt changed, stop and report the drift.

## Status
- **Priority**: P1
- **Effort**: S
- **Risk**: LOW
- **Depends on**: none
- **Category**: tests
- **Planned at**: commit `c90e552`, 2026-07-15

## Why this matters
The working tree passes tests but fails lint and typecheck, so no later change has a trustworthy acceptance gate. This plan fixes only the two existing verification failures without changing runtime behavior.

## Current state
- `tests/unit/rpc.test.ts:166-168` has one assertion line Biome wants wrapped:
  ```ts
  const assertion = expect(heightPromise).rejects.toMatchObject({
    message: 'dogecoin rpc work queue exceeded at `http://***:***@dogecoin-rpc.example.com:22555/`',
  });
  ```
- `tests/unit/mempool-appear-detector.test.ts:89-91` assigns a callback that may return `void` to a port requiring `Promise<void>`:
  ```ts
  async start(handler) {
    onRawTx = handler;
  },
  ```
- Formatting uses Biome, 2 spaces, single quotes, 100 columns. Tests use Vitest.

## Commands you will need
- `bun run lint` → exit 0.
- `bun run typecheck` → exit 0, no diagnostics.
- `bun run test` → 122 or more tests pass; only intentional opt-in suites skip.
- `bun run ci` → exit 0.

## Scope
**In scope**
- `tests/unit/rpc.test.ts`
- `tests/unit/mempool-appear-detector.test.ts`
- `plans/README.md` (status only)

**Out of scope**
- Runtime source files.
- Test behavior, fixtures, assertions, or timing beyond the callback type correction.
- Formatting unrelated files.

## Steps
### Step 1: Apply the exact Biome wrapping
Run `bunx biome format --write tests/unit/rpc.test.ts` or manually apply only the reported wrapping.

**Verify**: `bunx biome check tests/unit/rpc.test.ts` → exit 0.

### Step 2: Make the stored ZMQ handler promise-compatible
Wrap the assigned handler so `onRawTx` always has `(hex: string) => Promise<void>`, for example with an `async` adapter that awaits the incoming callback. Preserve the test's behavior and invocation.

**Verify**: `bun run typecheck` → exit 0.

### Step 3: Run the full baseline
Run lint, typecheck, tests, then the composed gate.

**Verify**: `bun run ci` → exit 0.

## Test plan
- Keep the existing RPC masking assertion unchanged.
- Keep the ZMQ “skip decode with no watches” assertion unchanged.
- Run `bunx vitest run tests/unit/rpc.test.ts tests/unit/mempool-appear-detector.test.ts` → both files pass.

## Done criteria
- [ ] `bun run ci` exits 0.
- [ ] No runtime file changed.
- [ ] `git diff --check` exits 0.
- [ ] Only in-scope files plus `plans/README.md` are modified by this plan.

## STOP conditions
- Either failure moved into runtime code or requires changing a production interface.
- A targeted test fails after the type-only adjustment.
- Fixing the baseline requires updating snapshots or dependencies.

## Maintenance notes
Keep this plan first in execution order. Every later plan assumes `bun run ci` is green before work begins.
