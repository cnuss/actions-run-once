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
// Implementation notes:
//   - Dependency-free: no @actions/* packages, no node_modules, no build step.
//     Runs as a node24 action so the runner injects ACTIONS_RUNTIME_TOKEN /
//     ACTIONS_RESULTS_URL (it withholds them from `run:` shells).
//   - Every request uses a FRESH TCP socket (agent: false). The cache service
//     sits behind eventually-consistent read replicas; a keep-alive connection
//     pins a poller to ONE replica, so if that replica lags behind the winner's
//     finalize the loser can stall for minutes. A new socket per poll lets the
//     load balancer rotate us onto a replica that has already ingested it.

const fs = require('fs');
const https = require('https');
const crypto = require('crypto');
const { URL } = require('url');
const { spawnSync } = require('child_process');

const ENVELOPE_VERSION = 'run-once-v1';
// Losers don't poll serially. They keep POLL_WIDTH probes in flight at once,
// each on a fresh socket so they fan across the cache's read replicas; the
// first replica that has ingested the winner's finalize wins the race. This
// turns convergence from "slowest replica I happen to poll" into "fastest
// replica in the fleet". RELAUNCH_DELAY_MS paces each slot after a miss.
// Empirically, convergence is propagation-bound (~tens of seconds until the
// replica fleet goes consistent), NOT sampling-bound: probing harder than this
// doesn't converge faster, it just adds load. A lean swarm gets the same tail
// as an aggressive one with ~5x fewer requests. 8-wide @ 1s is the knee.
const POLL_WIDTH = 8;
const RELAUNCH_DELAY_MS = 1000;
const BACKOFF_DELAY_MS = 3000; // when the service rate-limits / errors (429/5xx)
const DOWNLOAD_RETRIES = 6;
const DOWNLOAD_RETRY_DELAY_MS = 1000;

let DEBUG = false;

function log(msg) { process.stdout.write(`${msg}\n`); }
function fail(msg) { log(`::error::${msg}`); process.exitCode = 1; }
function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

// Strip Azure SAS secrets (and any token-ish query params) before logging URLs.
function redactUrl(urlStr) {
  try {
    const u = new URL(urlStr);
    for (const p of ['sig', 'skoid', 'sktid']) {
      if (u.searchParams.has(p)) u.searchParams.set(p, 'REDACTED');
    }
    return `${u.origin}${u.pathname}${u.search}`;
  } catch { return urlStr; }
}

// Per-request debug line: method, redacted URL, status, response headers, and a
// truncated body. Headers often carry x-github-request-id / x-ms-request-id /
// date which help correlate which backend replica served a probe.
function logRequest(method, urlStr, res) {
  if (!DEBUG) return;
  const body = (res.text || '').replace(/\s+/g, ' ').trim().slice(0, 300);
  log(`::group::[req] ${method} ${res.status} ${redactUrl(urlStr)}`);
  log(`[req] headers: ${JSON.stringify(res.headers)}`);
  log(`[req] body: ${body}${(res.text || '').length > 300 ? ' …(truncated)' : ''}`);
  log('::endgroup::');
}

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

// One request, one fresh socket. No keep-alive -> no replica stickiness.
function request(method, urlStr, headers, body) {
  return new Promise((resolve, reject) => {
    const u = new URL(urlStr);
    const data = body == null ? null : (Buffer.isBuffer(body) ? body : Buffer.from(body));
    const opts = {
      method,
      hostname: u.hostname,
      port: u.port || 443,
      path: u.pathname + u.search,
      headers: { 'Connection': 'close', ...headers },
      agent: false,
    };
    if (data) opts.headers['Content-Length'] = data.length;
    const req = https.request(opts, (res) => {
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => {
        const buf = Buffer.concat(chunks);
        const out = { status: res.statusCode, text: buf.toString('utf8'), buffer: buf, headers: res.headers };
        logRequest(method, urlStr, out);
        resolve(out);
      });
    });
    req.on('error', reject);
    if (data) req.write(data);
    req.end();
  });
}

async function twirp(base, method, body, token) {
  const res = await request('POST', `${base}/${method}`, {
    'Authorization': `Bearer ${token}`,
    'Content-Type': 'application/json',
  }, JSON.stringify(body));
  let json = {};
  try { json = res.text ? JSON.parse(res.text) : {}; } catch { /* leave {} */ }
  return { status: res.status, json, text: res.text };
}

async function main() {
  const key = getInput('key');
  const script = getInput('run');
  const timeoutSeconds = parseInt(getInput('timeout-seconds') || '600', 10);
  DEBUG = /^(true|1)$/i.test(getInput('debug')) || process.env.ACTIONS_STEP_DEBUG === 'true';
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

  const put = await request('PUT', uploadUrl, {
    'x-ms-blob-type': 'BlockBlob',
    'Content-Type': 'application/octet-stream',
  }, bytes);
  if (put.status < 200 || put.status >= 300) return fail(`blob upload failed: HTTP ${put.status}`);

  const final = await twirp(base, 'FinalizeCacheEntryUpload',
    { key, version, size_bytes: bytes.length }, token);
  if (final.json.ok !== true) return fail(`FinalizeCacheEntryUpload not ok: ${final.text}`);
  log(`[run-once] finalized key="${key}" (${bytes.length} bytes), exit=${exit}`);

  setOutput('output', stdout);
  if (exit !== 0) fail(`winning script exited ${exit}`);
}

// Keep POLL_WIDTH concurrent GetCacheEntryDownloadURL probes in flight (each a
// fresh socket -> different replica). Resolve with the download URL from the
// first probe that sees the finalized entry, or '' on timeout. No awaits in a
// loop: the swarm replenishes itself via promise callbacks.
function raceForVisibility({ base, token, key, version, deadline }) {
  return new Promise((resolve) => {
    const start = Date.now();
    let settled = false;
    let inflight = 0;
    let polls = 0;

    const settle = (url) => { if (!settled) { settled = true; resolve(url); } };

    const launch = () => {
      if (settled) return;
      if (Date.now() >= deadline) { if (inflight === 0) settle(''); return; }
      inflight += 1;
      polls += 1;
      const n = polls;
      twirp(base, 'GetCacheEntryDownloadURL', { key, version, restore_keys: [] }, token)
        .then((dl) => {
          inflight -= 1;
          if (settled) return;
          const url = dl.json.ok === true && (dl.json.signed_download_url || dl.json.signedDownloadUrl);
          if (url) {
            log(`[run-once] entry visible after ${n} probes / ${Math.round((Date.now() - start) / 1000)}s`);
            settle(url);
            return;
          }
          if (n % 40 === 0) log(`[run-once] still waiting (${n} probes, ${Math.round((Date.now() - start) / 1000)}s)`);
          // Back off if the service is rate-limiting / erroring; otherwise keep
          // the swarm tight to sample replicas fast.
          const delay = dl.status === 429 || dl.status >= 500 ? BACKOFF_DELAY_MS : RELAUNCH_DELAY_MS;
          setTimeout(launch, delay);
        })
        .catch(() => {
          inflight -= 1;
          if (!settled) setTimeout(launch, RELAUNCH_DELAY_MS);
        });
    };

    for (let i = 0; i < POLL_WIDTH; i += 1) launch();
  });
}

// Recursive retry (no loop): blob bytes can lag the metadata. Resolves the body
// text, or '' if every attempt failed.
function downloadEnvelope(url, attempt = 1) {
  return request('GET', url, {}, null).then((res) => {
    if (res.status === 200) return res.text;
    if (attempt >= DOWNLOAD_RETRIES) {
      log(`::error::blob download failed after ${attempt} tries: HTTP ${res.status}`);
      return '';
    }
    log(`[run-once] blob not ready (HTTP ${res.status}), retry ${attempt}/${DOWNLOAD_RETRIES}`);
    return sleep(DOWNLOAD_RETRY_DELAY_MS).then(() => downloadEnvelope(url, attempt + 1));
  });
}

async function runAsLoser({ base, token, key, version, timeoutSeconds }) {
  const deadline = Date.now() + timeoutSeconds * 1000;
  const downloadUrl = await raceForVisibility({ base, token, key, version, deadline });
  if (!downloadUrl) {
    return fail(`timed out after ${timeoutSeconds}s waiting for key="${key}" to finalize`);
  }

  const text = await downloadEnvelope(downloadUrl);
  if (!text) return fail('could not download winner output');

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
