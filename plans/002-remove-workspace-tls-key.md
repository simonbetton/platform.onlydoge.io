# Plan 002: Remove workspace TLS key material and prevent recurrence

> **Executor instructions**: Never open, print, diff, or copy the credential contents. Work only with filenames. Follow every verification gate and update `plans/README.md` when complete.
>
> **Drift check (run first)**: `git diff --stat c90e552..HEAD -- .gitignore`
> Also run `git ls-files -- macbook-pro.tail22aff3.ts.net.key macbook-pro.tail22aff3.ts.net.crt`. Stop if either file became tracked.

## Status
- **Priority**: P1
- **Effort**: S
- **Risk**: LOW
- **Depends on**: none
- **Category**: security
- **Planned at**: commit `c90e552`, 2026-07-15

## Why this matters
The repository root contains an untracked TLS private key and certificate. They are absent from Git history, but an accidental broad add would expose host identity material.

## Current state
- Both files physically exist in the repository root at planning time. Do not read them.
- `git log --all -- <filenames>` returned no history at planning time.
- `.gitignore:9-10` now contains broad local certificate protection:
  ```gitignore
  *.crt
  *.key
  ```

## Commands you will need
- `test ! -e macbook-pro.tail22aff3.ts.net.key && test ! -e macbook-pro.tail22aff3.ts.net.crt` → exit 0 after relocation/removal.
- `git check-ignore -v macbook-pro.tail22aff3.ts.net.key macbook-pro.tail22aff3.ts.net.crt` → identifies `.gitignore:9-10`.
- `git log --all --oneline -- macbook-pro.tail22aff3.ts.net.key macbook-pro.tail22aff3.ts.net.crt` → no output.
- `bun run ci` → exit 0 (after Plan 001).

## Scope
**In scope**
- `.gitignore`
- Removing or relocating the two named local TLS files outside the repository.
- `plans/README.md` (status only)

**Out of scope**
- Reading credential contents.
- Adding certificates or keys to Git, plans, docs, fixtures, or env examples.
- Rotating unrelated application credentials.

## Steps
### Step 1: Confirm exposure history without reading files
Check tracked state and all Git history by filename. If history is empty, document that rotation is only required if the files were shared or backed up insecurely.

**Verify**: both `git ls-files -- macbook-pro.tail22aff3.ts.net.key macbook-pro.tail22aff3.ts.net.crt` and the concrete `git log` command above produce no output.

### Step 2: Relocate or remove local material
Move the files to an operator-controlled directory outside the repository, or delete them if regenerable. Do not display their contents.

**Verify**: the concrete `test ! -e ...` command above exits 0. Do not use `git status` for this check because ignored files can remain physically present.

### Step 3: Verify the existing ignore rule
Keep the existing `*.key` and `*.crt` rules unless the repository adds legitimate certificate fixtures. If narrowing becomes necessary, replace them with the two exact host-specific filenames.

**Verify**: create empty temporary files with the same names, run `git check-ignore -v` to confirm, then remove the temporary files without opening any real credential.

### Step 4: Decide rotation
If either file was ever shared, uploaded, backed up to an untrusted location, or added to another repository, rotate/reissue the host certificate outside this code change.

**Verify**: record the operator decision in the execution handoff without credential values.

## Test plan
No application behavior changes. Run `bun run ci` as a regression gate.

## Done criteria
- [ ] Named key/cert files are absent from the repository working tree.
- [ ] Exact ignore protection is present.
- [ ] Git history contains neither filename.
- [ ] Rotation decision is recorded without secret content.
- [ ] `bun run ci` exits 0.

## STOP conditions
- Either file is tracked or appears in Git history.
- A broad ignore rule would hide legitimate committed fixtures.
- Safe relocation requires exposing or printing the key.

## Maintenance notes
If tracked history is discovered later, removal is insufficient: rotate first, then follow the repository's history-rewrite policy with explicit maintainer approval.
