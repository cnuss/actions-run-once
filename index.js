'use strict';

// Distributed run-once / leader election on the Actions cache service.
//
// All jobs invoking this action with the same `key` race to reserve a cache
// entry via CreateCacheEntry. The reservation is atomic: exactly one job wins
// (gets a signed upload URL); the rest get `already_exists`. The winner runs
// the `run` script, uploads its stdout, and finalizes — which is the moment the
// entry becomes visible. Losers poll GetCacheEntryDownloadURL until the entry
// is finalized, then download the winner's output. Everyone ends with the same
// `output`, and a non-zero winner exit is mirrored to every racer.
//
// Dependency-free: no @actions/* packages, no node_modules, no build step.
// Runs as a node24 action so the runner injects ACTIONS_RUNTIME_TOKEN /
// ACTIONS_RESULTS_URL into this process (it withholds them from `run:` shells).

const fs = require('fs');
const crypto = require('crypto');
const { spawnSync } = require('child_process');

const ENVELOPE_VERSION = 'run-once-v1';
const POLL_INTERVAL_MS = 2000;

function log(msg) { process.stdout.write(`${msg}\n`); }
function fail(msg) { log(`::error::${msg}`); process.exitCode = 1; }

function getInput(name) {
  const v = process.env[`INPUT_${name.toUpperCase().replace(/ /g, '_')}`];
  return v === undefined ? '' : v;
}

function setOutput(name, value) {
  const file = process.env.GITHUB_OUTPUT;
  if (!file) return;
  const delimiter = 'ghadelimiter_' + crypto.randomBytes(16).toString('hex');
  fs.appendFileSync(file, `${name}<<${delimiter}\n${value ?? ''}\n${delimiter}\n`);
}

// version is the cache service's silent matcher; derive it from the user key so
// distinct keys never collide and the same key always lines up.
function versionFor(key) {
  return crypto.createHash('sha256').update(`${ENVELOPE_VERSION}:${key}`).digest('hex');
}

async function twirp(base, method, body, token) {
  const res = await fetch(`${base}/${method}`, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
  });
  const text = await res.text();
  let json = {};
  try { json = text ? JSON.parse(text) : {}; } catch { /* leave {} */ }
  return { status: res.status, json, text };
}

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

async function main() {
  const key = getInput('key');
  const script = getInput('run');
  const timeoutSeconds = parseInt(getInput('timeout-seconds') || '600', 10);
  if (!key) return fail('input `key` is required');
  if (!script) return fail('input `run` is required');

  const token = process.env.ACTIONS_RUNTIME_TOKEN;
  const resultsUrl = process.env.ACTIONS_RESULTS_URL;
  if (!token || !resultsUrl) {
    return fail('ACTIONS_RUNTIME_TOKEN / ACTIONS_RESULTS_URL not present — is this running as an action in GitHub Actions?');
  }
  const base = `${resultsUrl.replace(/\/$/, '')}/twirp/github.actions.results.api.v1.CacheService`;
  const version = versionFor(key);

  // ---- 1. Race to reserve via CreateCacheEntry --------------------------
  const create = await twirp(base, 'CreateCacheEntry', { key, version }, token);
  const uploadUrl = create.json.signed_upload_url || create.json.signedUploadUrl;

  if (uploadUrl) {
    log(`[run-once] WON reservation for key="${key}" — running script`);
    return runAsWinner({ base, token, key, version, script, uploadUrl });
  }

  if (create.json.code === 'already_exists') {
    log(`[run-once] lost reservation for key="${key}" — waiting for winner`);
    return runAsLoser({ base, token, key, version, timeoutSeconds });
  }

  return fail(`unexpected CreateCacheEntry response (status ${create.status}): ${create.text}`);
}

async function runAsWinner({ base, token, key, version, script, uploadUrl }) {
  // Run the script. Stream its output to our log so it's visible live-ish.
  const child = spawnSync('bash', ['-c', script], {
    encoding: 'utf8',
    maxBuffer: 64 * 1024 * 1024,
  });
  const exit = child.status === null ? 1 : child.status;
  const stdout = child.stdout || '';
  if (stdout) process.stdout.write(stdout);
  if (child.stderr) process.stderr.write(child.stderr);
  if (child.error) log(`[run-once] script spawn error: ${child.error.message}`);

  // Envelope carries exit + stdout so losers reproduce both.
  const envelope = JSON.stringify({ v: ENVELOPE_VERSION, exit, output: stdout });
  const bytes = Buffer.from(envelope, 'utf8');

  const put = await fetch(uploadUrl, {
    method: 'PUT',
    headers: { 'x-ms-blob-type': 'BlockBlob', 'Content-Type': 'application/octet-stream' },
    body: bytes,
  });
  if (!put.ok) return fail(`blob upload failed: HTTP ${put.status}`);

  const final = await twirp(base, 'FinalizeCacheEntryUpload',
    { key, version, size_bytes: bytes.length }, token);
  if (final.json.ok !== true) return fail(`FinalizeCacheEntryUpload not ok: ${final.text}`);
  log(`[run-once] finalized key="${key}" (${bytes.length} bytes), exit=${exit}`);

  setOutput('output', stdout);
  if (exit !== 0) fail(`winning script exited ${exit}`);
}

async function runAsLoser({ base, token, key, version, timeoutSeconds }) {
  const deadline = Date.now() + timeoutSeconds * 1000;
  let downloadUrl = '';
  while (Date.now() < deadline) {
    const dl = await twirp(base, 'GetCacheEntryDownloadURL',
      { key, version, restore_keys: [] }, token);
    if (dl.json.ok === true) {
      downloadUrl = dl.json.signed_download_url || dl.json.signedDownloadUrl;
      break;
    }
    await sleep(POLL_INTERVAL_MS);
  }
  if (!downloadUrl) {
    return fail(`timed out after ${timeoutSeconds}s waiting for key="${key}" to finalize`);
  }

  const res = await fetch(downloadUrl);
  if (!res.ok) return fail(`blob download failed: HTTP ${res.status}`);
  const text = await res.text();
  let envelope;
  try { envelope = JSON.parse(text); } catch { return fail(`winner envelope is not JSON: ${text.slice(0, 200)}`); }

  const output = envelope.output ?? '';
  const exit = envelope.exit ?? 0;
  if (output) process.stdout.write(output);
  log(`[run-once] reused winner output for key="${key}", exit=${exit}`);

  setOutput('output', output);
  if (exit !== 0) fail(`winning script exited ${exit} (propagated)`);
}

main().catch((err) => fail(err && err.stack ? err.stack : String(err)));
