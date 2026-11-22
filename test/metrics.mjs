#!/usr/bin/env node
/**
 * Measurement run (not an assertion suite).
 *
 * Produces the numbers `docs/evaluation.md` used to call "not measured":
 *   - write availability while the upstream is down
 *   - replay convergence time (explicit sync after recovery)
 *   - duplicate stability across repeated pulls
 *   - search latency at 1k rows (shows where the LIKE scorer stands)
 *
 *   node test/metrics.mjs
 *
 * It prints a markdown table on stdout; it does not assert, so a slow machine
 * reports slow numbers instead of failing CI.
 */
import { createServer } from 'node:http';
import { spawn } from 'node:child_process';
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, rmSync, openSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const REPO = join(HERE, '..');
const SECRET = 'metrics-secret';
const WRITES = Number(process.env.METRICS_WRITES || 200);
const CONVERGENCE_ROUNDS = Number(process.env.METRICS_ROUNDS || 5);
const ROUND_WRITES = 20;
const SEED_ROWS = Number(process.env.METRICS_SEED || 1000);
const SEARCHES = 50;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function freePort() {
  return new Promise((resolve, reject) => {
    const s = createServer();
    s.listen(0, '127.0.0.1', () => { const p = s.address().port; s.close(() => resolve(p)); });
    s.on('error', reject);
  });
}

async function waitFor(fn, ms = 20000, label = 'condition') {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    try { if (await fn()) return true; } catch { /* keep polling */ }
    await sleep(50);
  }
  throw new Error(`timeout waiting for ${label}`);
}

const percentile = (values, p) => {
  const sorted = [...values].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.max(0, Math.ceil((p / 100) * sorted.length) - 1));
  return sorted[idx];
};

function startStub(port) {
  const st = { online: false, memories: [], seq: 0 };
  const server = createServer(async (req, res) => {
    const send = (code, obj) => { res.writeHead(code, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(obj)); };
    const url = new URL(req.url, 'http://x');
    if (url.pathname === '/agentmemory/livez') return st.online ? send(200, { status: 'ok' }) : send(503, { status: 'down' });
    if (req.headers.authorization !== `Bearer ${SECRET}`) return send(401, { error: 'unauthorized' });
    if (!st.online) return send(503, { error: 'offline' });
    let raw = '';
    for await (const chunk of req) raw += chunk;
    const body = raw ? JSON.parse(raw) : {};
    if (url.pathname === '/agentmemory/export') return send(200, { memories: st.memories });
    if (url.pathname === '/agentmemory/remember') {
      const m = { id: `up_${++st.seq}`, ...body, createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() };
      st.memories.push(m);
      return send(200, { success: true, memory: m });
    }
    if (url.pathname === '/agentmemory/forget') {
      st.memories = st.memories.filter((m) => m.id !== body.memoryId);
      return send(200, { success: true });
    }
    return send(404, {});
  });
  return new Promise((resolve) => server.listen(port, '127.0.0.1', () => resolve({ st, server })));
}

function startHub(port, upstream, work) {
  mkdirSync(join(work, 'snap'), { recursive: true });
  mkdirSync(join(work, 'skills'), { recursive: true });
  writeFileSync(join(work, 'secret'), SECRET + '\n');
  const cfg = { port, upstream, secretFile: join(work, 'secret'), db: 'hub.db', syncIntervalMs: 3600000, snapDir: './snap', skillsDir: join(work, 'skills') };
  const cfgPath = join(work, 'config.json');
  writeFileSync(cfgPath, JSON.stringify(cfg));
  const env = { ...process.env };
  for (const k of ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']) delete env[k];
  const log = openSync(join(work, 'hub.log'), 'a');
  const proc = spawn(process.execPath, [join(REPO, 'src', 'hub.mjs'), cfgPath], { cwd: REPO, env, stdio: ['ignore', log, log] });
  return proc;
}

function api(base) {
  return async (method, path, body) => {
    const res = await fetch(base + path, {
      method,
      headers: { Authorization: `Bearer ${SECRET}`, 'Content-Type': 'application/json' },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    const text = await res.text();
    let json = null;
    try { json = text ? JSON.parse(text) : null; } catch { json = { raw: text }; }
    return { status: res.status, body: json };
  };
}

const work = mkdtempSync(join(tmpdir(), 'memhub-metrics-'));
let hub = null;
let upstream = null;
try {
  const [hubPort, upPort] = [await freePort(), await freePort()];
  upstream = await startStub(upPort);
  hub = startHub(hubPort, `http://127.0.0.1:${upPort}`, work);
  const base = `http://127.0.0.1:${hubPort}`;
  const A = api(base);
  await waitFor(async () => (await A('GET', '/agentmemory/health')).status === 200, 30000, 'hub start');

  const syncStable = async () => {
    for (let i = 0; i < 60; i++) {
      const r = (await A('POST', '/agentmemory/sync')).body;
      if (!r.skipped) return r;
      await sleep(50);
    }
    throw new Error('sync never left the in-flight state');
  };

  // ---- M1: write availability while the upstream is down -------------------
  const writeMs = [];
  let ok = 0;
  for (let i = 1; i <= WRITES; i++) {
    const t = performance.now();
    const res = await A('POST', '/agentmemory/remember', { title: `probe ${i}`, content: `offline write probe ${i}`, tags: ['metrics'] });
    writeMs.push(performance.now() - t);
    if (res.status === 200) ok++;
  }
  const livezOffline = (await A('GET', '/agentmemory/livez')).body;

  // ---- M2: replay convergence after recovery ------------------------------
  const convergence = [];
  const roundSizes = [];
  const passesPerRound = [];
  for (let round = 0; round < CONVERGENCE_ROUNDS; round++) {
    upstream.st.online = false;
    const n = round === 0 ? 0 : ROUND_WRITES; // round 0 uses the backlog from M1
    for (let i = 0; i < n; i++) {
      await A('POST', '/agentmemory/remember', { title: `round ${round} ${i}`, content: `round ${round} write ${i}`, tags: ['metrics'] });
    }
    const pending = (await A('GET', '/agentmemory/livez')).body.pendingWrites;
    upstream.st.online = true;
    const t = performance.now();
    // pushOutbox drains at most 100 rows per pass, so convergence is "keep
    // syncing until the queue is empty" -- the 60 s timer would do the same,
    // just once per minute.
    let passes = 0;
    while (true) {
      await syncStable();
      passes++;
      if ((await A('GET', '/agentmemory/livez')).body.pendingWrites === 0) break;
      if (passes >= 60) throw new Error(`queue did not drain after ${passes} passes`);
    }
    convergence.push(performance.now() - t);
    roundSizes.push(pending);
    passesPerRound.push(passes);
  }

  // ---- M3: duplicate stability across repeated pulls ----------------------
  await syncStable();
  const rowsBefore = (await A('GET', '/agentmemory/memories?limit=500')).body.total;
  const addedPerPull = [];
  for (let i = 0; i < 5; i++) addedPerPull.push((await syncStable()).pull.added);
  const rowsAfter = (await A('GET', '/agentmemory/memories?limit=500')).body.total;

  // ---- M4: search latency at 1k rows (bypasses the outbox via import) -----
  const seed = Array.from({ length: SEED_ROWS }, (_, i) => ({
    id: `seed_${i}`,
    title: `seed row ${i}`,
    content: `seed content number ${i} for the latency probe`,
    tags: ['seed'],
    scope: 'global',
  }));
  for (let i = 0; i < seed.length; i += 200) {
    await A('POST', '/agentmemory/import', { memories: seed.slice(i, i + 200) });
  }
  const searchMs = [];
  for (let i = 0; i < SEARCHES; i++) {
    const t = performance.now();
    await A('POST', '/agentmemory/smart-search', { query: `seed ${i}`, limit: 5 });
    searchMs.push(performance.now() - t);
  }

  const pct = (arr, p) => percentile(arr, p).toFixed(2);
  const livezFinal = (await A('GET', '/agentmemory/livez')).body;
  const snapCount = JSON.parse(readFileSync(join(work, 'snap', 'latest.json'), 'utf8')).count;
  console.log('\n| 指标 | 口径 | 实测 |');
  console.log('|---|---|---|');
  console.log(`| 写入可用性 | 上游离线，连续 ${WRITES} 次 remember | **${ok}/${WRITES} = ${((ok / WRITES) * 100).toFixed(1)}%** |`);
  console.log(`| 写入延迟（离线） | 同上，端到端 P50 / P95 | ${pct(writeMs, 50)} ms / ${pct(writeMs, 95)} ms |`);
  console.log(`| 补推收敛 | 上游恢复后显式 sync，待推量归零 | ${CONVERGENCE_ROUNDS} 轮：${convergence.map((v) => v.toFixed(0)).join(' / ')} ms（积压 ${roundSizes.join(' / ')} 条，${passesPerRound.join(' / ')} 次 sync） |`);
  console.log(`| 去重稳定性 | 连续 5 次 sync 的 pull.added | ${addedPerPull.join(' / ')}（行数 ${rowsBefore} → ${rowsAfter}） |`);
  console.log(`| 检索延迟（${SEED_ROWS} 行） | smart-search 端到端 P50 / P95 | ${pct(searchMs, 50)} ms / ${pct(searchMs, 95)} ms |`);
  console.log(`| 快照一致性 | livez.memories 与 snapshot.count | ${livezFinal.memories} == ${snapCount}（${livezFinal.memories === snapCount ? '一致' : '不一致'}） |`);
  console.log(`\n（口径：本机单进程、随机端口、stub 上游；离线期间 pendingWrites=${livezOffline.pendingWrites}，不经隧道、不含真实网络。）`);
} catch (e) {
  console.error('metrics failed:', e.message);
  process.exitCode = 1;
} finally {
  try { hub?.kill(); } catch {}
  try { upstream?.server.close(); } catch {}
  await sleep(150);
  try { rmSync(work, { recursive: true, force: true }); } catch {}
}
