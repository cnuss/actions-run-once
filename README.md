# actions-run-once

Run a bash script **exactly once** across concurrent GitHub Actions jobs that
share a key — distributed run-once / leader election, built on the Actions
cache service.

One job wins an atomic reservation, runs your script, and caches its stdout.
Every other job with the same key downloads that same output instead of
re-running. A non-zero exit on the winner is propagated to every racer.

## Why

The cache service's `CreateCacheEntry` is an atomic reservation: given N
concurrent callers on the same key+version, exactly one gets a write URL and the
rest get `already_exists`. The entry only becomes visible on
`FinalizeCacheEntryUpload`. That's a leader-election lock with a published
result — this action wraps it.

## Inputs

| input | required | default | description |
|---|---|---|---|
| `key` | yes | | All jobs sharing this key elect one winner. Make it unique to the work (e.g. include a run id, or a content hash to memoize across runs). |
| `run` | yes | | Bash script run only on the winner. Its stdout becomes `output`. |
| `timeout-seconds` | no | `600` | How long a losing job waits for the winner to finalize before failing. |

## Outputs

| output | description |
|---|---|
| `output` | Stdout of the winning job's script — identical for every racer. |

## Usage

```yaml
jobs:
  build:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        shard: [1, 2, 3, 4]
    steps:
      - id: once
        uses: cnuss/actions-run-once@v1
        with:
          key: expensive-step-${{ github.run_id }}
          run: |
            echo "computing once for all shards..."
            date -u +%s

      - run: echo "every shard sees: ${{ steps.once.outputs.output }}"
```

All four shards call the action; one runs the script, the other three reuse its
output. Useful for: one-time setup across a matrix, single migration/seed,
generating a shared build id, or memoizing an expensive computation by content
hash.

## How it works

1. `CreateCacheEntry { key, version }` — atomic race. Winner gets a signed
   upload URL; losers get `already_exists`.
2. **Winner**: runs `bash -c "$run"`, PUTs a JSON envelope `{exit, output}` to
   the blob, then `FinalizeCacheEntryUpload` (which publishes the entry).
3. **Losers**: poll `GetCacheEntryDownloadURL` until finalized, download the
   envelope, set `output`, and mirror the winner's exit code.

`version` is `sha256("run-once-v1:" + key)`, so distinct keys never collide.

## Notes

- Dependency-free: no `node_modules`, no bundling, no build step. Node 24.
- Needs `permissions: actions: write` in the calling job (cache write).
- The reservation is immutable: once a key finalizes, later runs reuse it until
  the cache entry is evicted or deleted. Use a per-run key if you want fresh
  execution each run.
