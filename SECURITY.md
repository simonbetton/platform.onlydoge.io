# Security

## Supported Branches

Security fixes are handled on the default branch until the project starts publishing versioned support windows.

## Reporting a Vulnerability

Please do not open public issues for suspected vulnerabilities. Use GitHub private vulnerability reporting if it is enabled for the repository; otherwise contact a maintainer privately before publishing details.

Include:

- the affected component or endpoint,
- reproduction steps,
- expected impact,
- any relevant logs with secrets redacted.

## Secret Handling

Do not commit real `.env` files, API keys, database URLs with live credentials, RPC credentials, private keys, SSH keys, or cloud access keys.

The repository intentionally tracks only example env files:

- `.env.local.example`
- `.env.production.example`
- `.env.managed.example`

Real env files are ignored by `.gitignore`.

Before publishing a branch or release, run a local secret scan. This is a high-signal regex check, not a replacement for reviewing the diff:

```bash
git ls-files -z | xargs -0 rg -n --hidden \
  --glob '!bun.lock' \
  --glob '!tests/fixtures/**' \
  --glob '!tests/integration/__snapshots__/**' \
  '(AKIA[0-9A-Z]{16}|ASIA[0-9A-Z]{16}|ghp_[A-Za-z0-9_]{20,}|github_pat_[A-Za-z0-9_]{20,}|xox[baprs]-[A-Za-z0-9-]{10,}|sk[_-][A-Za-z0-9]{20,}|-----BEGIN (RSA|OPENSSH|EC|DSA|PRIVATE) KEY-----)'
```

Also inspect any changed `.env*.example`, Docker Compose files, deploy scripts, and docs for production hostnames, private IPs, bucket names, and credential-shaped placeholders.
