# Plan 022: Align Bun runtime, CI and type versions

> **Executor instructions**: Select the already-shipped production version unless compatibility testing requires a newer deliberate upgrade. Update all surfaces in one change.
>
> **Drift check (run first)**: `git diff --stat c90e552..HEAD -- package.json Dockerfile .github/workflows README.md bun.lock`

## Status
- **Priority**: P2
- **Effort**: S
- **Risk**: LOW
- **Depends on**: `plans/001-restore-verification-baseline.md`
- **Category**: dependencies/DX
- **Planned at**: commit `c90e552`, 2026-07-15

## Why this matters
CI/packageManager use Bun 1.3.6 while the production image and `bun-types` use 1.3.14. A green check can therefore exercise different runtime and type APIs than the shipped container.

## Current state
- `package.json:6` declares `packageManager: bun@1.3.6`.
- `package.json:65` declares `bun-types` 1.3.14.
- `Dockerfile:1` ships `oven/bun:1.3.14-alpine`.
- `.github/workflows/ci.yml:17-31`, `.github/workflows/security.yml:20`,
  `.github/workflows/publish-image.yml:25`, and `.github/workflows/deploy-production.yml:22,69`
  install Bun 1.3.6.

## Scope
**In scope**
- `package.json`, lock metadata if changed.
- Dockerfile and all GitHub workflows that install Bun.
- README/tool-version docs.
- A version-consistency test/script.
- `plans/README.md` (status only)

**Out of scope**
- Upgrading TypeScript or unrelated packages.
- Changing Alpine/base image family.

## Steps
1. Choose one Bun patch version after reviewing release notes. Default recommendation: 1.3.14, because production and `bun-types` already use it.
2. Align `packageManager`, `bun-types`, Docker base tag, CI, security, publish and deploy workflows.
3. Add a small read-only consistency check that parses these files and fails when versions diverge. Avoid duplicating parsing logic in multiple workflows.
4. Reinstall only through Bun to refresh lock metadata if required, then run the full gate and Docker build.
5. Document the single upgrade checklist.

## Verification
- Version search finds one Bun version across runtime/type/tooling declarations.
- `bun --version` in CI and built production image equals the selected version.
- `bun install --frozen-lockfile`, `bun run ci`, and production Docker build pass.

## Done criteria
- [ ] CI, deploy, publish, Docker, packageManager and bun-types align.
- [ ] Drift check is automated.
- [ ] No unrelated dependency upgrades are present.

## STOP conditions
- `bun-types` patch numbering is not intended to match runtime for the selected release; document the supported pairing instead of forcing equality.
- Production behavior regresses on the aligned version; revert version selection and report evidence.

## Maintenance notes
Future Bun upgrades are atomic cross-file changes and must include the production image build.
