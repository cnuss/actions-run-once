# Security Policy

## Supported versions

| Version | Supported |
|---------|-----------|
| `v1.x` (tag `v1`) | :white_check_mark: |
| `< v1` | :x: |

Always pin to a released tag — `uses: cnuss/actions-run-once@v1` (moving major)
or a full commit SHA for maximum supply-chain safety.

## Reporting a vulnerability

**Do not open a public issue for security problems.**

Report privately via GitHub's **Private Vulnerability Reporting**:

1. Go to the repository's **Security** tab.
2. Click **Report a vulnerability** (Advisories → Report).
3. Describe the issue, affected versions, and reproduction steps.

You will get an acknowledgement within **3 business days**. Once confirmed, a
fix and a GitHub Security Advisory (with CVE if warranted) will be published,
and the moving `v1` tag updated.

## Scope

This action runs a caller-supplied bash script (`run`) on the winning job and
caches its stdout, then serves that output to the other racers via the Actions
cache service. It reads runner-injected environment (`ACTIONS_RUNTIME_TOKEN`,
`ACTIONS_RESULTS_URL`) and uses the caller's own short-lived job token, which
expires with the job. It holds no secrets of its own and exfiltrates nothing
off-runner.

In scope: code execution beyond the supplied `run` script, output injection,
token leakage to logs, cache-entry tampering / poisoning across keys, or
supply-chain tampering with releases/tags.

Out of scope: the action faithfully running the `run` script a workflow author
provided (including one that prints secrets it was given), and the latency /
eventual-consistency characteristics of the underlying cache service.
