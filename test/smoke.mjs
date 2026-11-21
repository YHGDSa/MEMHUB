#!/usr/bin/env node
/**
 * End-to-end smoke test for the local-first hub.
 *
 * Runs a throwaway hub (random port, temp database) against a stub upstream,
 * then walks the full contract: offline write, offline search, scope isolation,
 * redaction, replay after recovery, pull de-duplication, delete (online and
 * while the outbox still holds the write), import/export, skill indexing and
 * snapshot freshness.
 *
 * Nothing here touches a real upstream, a real database or a real port.
 *
 *   node test/smoke.mjs          # run, clean up
 *   node test/smoke.mjs --keep   # keep the temp dir for inspection
 *
 * Exit code 0 = every assertion passed.
 */
import { createServer } from 'node:http';
import { spawn } from 'node:child_process';
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, existsSync, rmSync, openSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const REPO = join(HERE, '..');
const KEEP = process.argv.includes('--keep');
const SECRET = 'smoke-test-secret';

/* ------------------------------------------------------------------ helpers */

const results = [];
function check(name, ok, detail = '') {
  results.push({ name, ok, detail });
  const mark = ok ? 'PASS' : 'FAIL';
  console.log(`  ${mark}  ${name}${detail ? `  (${detail})` : ''}`);
  return ok;
}
const eq = (name, actual, expected) =>
  check(name, JSON.stringify(actual) === JSON.stringify(expected), `got ${JSON.stringify(actual)}`);

async function freePort() {
  return new Promise((resolve, reject) => {
    const srv = createServer();
    srv.listen(0, '127.0.0.1', () => {
      const { port } = srv.address();
      srv.close(() => resolve(port));
    });
    srv.on('error', reject);
  });
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function waitFor(fn, timeoutMs = 15000, label = 'condition') {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      if (await fn()) return true;
    } catch { /* keep polling */ }
    await sleep(150);
  }
  throw new Error(`timeout waiting for ${label}`);
}

/* ------------------------------------------------------------- stub upstream */

function startStub(port) {
  const state = { online: false, memories: [], seq: 0 };
  const server = createServer(async (req, res) => {
    const send = (code, obj) => {
      const body = JSON.stringify(obj);
      res.writeHead(code, { 'Content-Type': 'application/json' });
      res.end(body);
    };
    const url = new URL(req.url, 'http://127.0.0.1');
    if (url.pathname === '/agentmemory/livez') {
      return state.online ? send(200, { status: 'ok' }) : send(503, { status: 'down' });
    }
    if (req.headers.authorization !== `Bearer ${SECRET}`) return send(401, { error: 'unauthorized' });
    if (!state.online) return send(503, { error: 'offline' });

    let raw = '';
    for await (const chunk of req) raw += chunk;
    const body = raw ? JSON.parse(raw) : {};

    if (url.pathname === '/agentmemory/export' && req.method === 'GET') {
      return send(200, { version: 'stub-1', memories: state.memories });
    }
    if (url.pathname === '/agentmemory/remember' && req.method === 'POST') {
      const id = `up_${++state.seq}`;
      const mem = {
        id,
        title: body.title,
        content: body.content,
        tags: body.tags || [],
        concepts: body.concepts || [],
        project: body.project ?? null,
        type: body.type || 'fact',
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
      };
      state.memories.push(mem);
      return send(200, { success: true, memory: mem });
    }
    if (url.pathname === '/agentmemory/forget' && req.method === 'POST') {
      const before = state.memories.length;
      state.memories = state.memories.filter((m) => m.id !== body.memoryId);
      return send(200, { success: true, removed: before - state.memories.length });
    }
    return send(404, { error: 'not found' });
  });
  return new Promise((resolve) => {
    server.listen(port, '127.0.0.1', () => resolve({ state, server }));
  });
}

/* --------------------------------------------------------------- hub client */

function client(base) {
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

/* --------------------------------------------------------------------- main */

const work = mkdtempSync(join(tmpdir(), 'memhub-smoke-'));
const hubDir = join(work, 'hub');
const snapDir = join(work, 'snap');
const skillsDir = join(work, 'skills');
const secretFile = join(work, 'secret');
mkdirSync(hubDir, { recursive: true });
mkdirSync(snapDir, { recursive: true });
mkdirSync(join(skillsDir, 'demo-skill'), { recursive: true });
writeFileSync(secretFile, SECRET + '\n', 'utf8');
writeFileSync(join(skillsDir, 'demo-skill', 'SKILL.md'), [
  '---',
  'name: demo-skill',
  'version: 1.2.0',
  'description: 演示用技能，验证技能索引与记忆同榜检索',
  'tags: demo,index',
  '---',
  '',
  '这个技能的正文应当可以被摘要检索命中。',
].join('\n'), 'utf8');

let hubProc = null;
let stub = null;
const cleanup = () => {
  try { hubProc?.kill(); } catch {}
  try { stub?.server.close(); } catch {}
  if (!KEEP) { try { rmSync(work, { recursive: true, force: true }); } catch {} }
};

try {
  const hubPort = await freePort();
  const upstreamPort = await freePort();
  stub = await startStub(upstreamPort);

  const config = {
    port: hubPort,
    upstream: `http://127.0.0.1:${upstreamPort}`,
    secretFile,
    db: 'hub.db',
    syncIntervalMs: 3600000, // the test syncs explicitly; never race a timer
    skillsDir,
  };
  const configPath = join(hubDir, 'config.json');
  writeFileSync(configPath, JSON.stringify(config, null, 2), 'utf8');

  const env = { ...process.env };
  for (const k of ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']) delete env[k];
  // File-backed stdio keeps the hub's log inspectable without a pipe.
  const hubLog = openSync(join(work, 'hub.log'), 'a');
  hubProc = spawn(process.execPath, [join(REPO, 'src', 'hub.mjs'), configPath], {
    cwd: REPO, env, stdio: ['ignore', hubLog, hubLog],
  });

  const base = `http://127.0.0.1:${hubPort}`;
  const api = client(base);
  await waitFor(async () => (await api('GET', '/agentmemory/health')).status === 200, 20000, 'hub start');

  // syncNow is single-flight inside the hub: a write triggers a background sync,
  // so an explicit one can come back as "skipped". Retry until it really ran.
  const syncStable = async () => {
    for (let i = 0; i < 40; i++) {
      const r = (await api('POST', '/agentmemory/sync')).body;
      if (!r.skipped) return r;
      await sleep(100);
    }
    throw new Error('sync never left the in-flight state');
  };

  console.log('\n1. contract surface');
  const noAuth = await fetch(`${base}/agentmemory/memories`);
  eq('unauthenticated request is rejected', noAuth.status, 401);
  const health = (await api('GET', '/agentmemory/health')).body;
  eq('hub is in local-hub mode', health.mode, 'local-hub');
  eq('skills index rebuilt from disk', health.skills, 1);

  console.log('\n2. offline write path (upstream down)');
  stub.state.online = false;
  const offline = await api('POST', '/agentmemory/remember', {
    title: '离线写入', content: '上游不可达时，本地必须先成功', tags: ['offline', 'smoke'], scope: 'global',
  });
  eq('write returns success while upstream is down', offline.body.success, true);
  const livez1 = (await api('GET', '/agentmemory/livez')).body;
  eq('hub reports upstream offline', livez1.upstream, 'offline');
  eq('memory stored locally', livez1.memories, 1);
  eq('write queued in outbox', livez1.pendingWrites, 1);
  const searchOffline = (await api('POST', '/agentmemory/smart-search', { query: '离线写入' })).body;
  check('offline search finds the memory', searchOffline.results.length === 1 && searchOffline.results[0].title === '离线写入');

  console.log('\n3. scope + redaction');
  const priv = await api('POST', '/agentmemory/remember', {
    title: '私密草稿', content: '本机可见，不应上浮', scope: 'private', tags: ['smoke'],
  });
  eq('private write stored', priv.status, 200);
  const livez2 = (await api('GET', '/agentmemory/livez')).body;
  eq('private write is not queued for upstream', livez2.pendingWrites, 1);
  eq('private write counts locally', livez2.memories, 2);

  const leak = 'token sk-abcdefghijklmnopqrstuvwx 与 https://user:pw@example.com/x 以及 Bearer abcdefghijklmnopqrstuvwxyz';
  const redacted = await api('POST', '/agentmemory/remember', { title: '凭据写入', content: leak, scope: 'global' });
  check('redaction reports hits', redacted.body.redacted >= 2, `redacted=${redacted.body.redacted}`);
  const storedBody = redacted.body.memory.content;
  check('stored content no longer holds the secret', !storedBody.includes('abcdefghijklmnopqrstuvwx'));
  check('stored content is masked', storedBody.includes('[REDACTED]'));

  console.log('\n4. recovery + replay');
  stub.state.online = true;
  const sync1 = await syncStable();
  check('sync reached upstream', sync1.upstream === true, JSON.stringify(sync1));
  const livez3 = (await api('GET', '/agentmemory/livez')).body;
  eq('outbox drained', livez3.pendingWrites, 0);
  eq('upstream received exactly the non-private writes', stub.state.memories.length, 2);
  check('upstream copy is already redacted', !stub.state.memories.some((m) => JSON.stringify(m).includes('abcdefghijklmnopqrstuvwx')));
  check('private write never reached upstream', !stub.state.memories.some((m) => m.title === '私密草稿'));

  console.log('\n5. pull de-duplication');
  const countBefore = (await api('GET', '/agentmemory/memories?limit=500')).body.total;
  const sync2 = await syncStable();
  eq('second sync pulls but adds nothing', sync2.pull.added, 0);
  const countAfter = (await api('GET', '/agentmemory/memories?limit=500')).body.total;
  eq('row count unchanged after re-pull', countAfter, countBefore);

  console.log('\n6. delete');
  const doomed = await api('POST', '/agentmemory/remember', { title: '待删除', content: '删除后两侧都应消失' });
  await syncStable();
  await waitFor(async () => (await api('GET', '/agentmemory/livez')).body.pendingWrites === 0, 5000, 'push of the doomed row');
  const upCountBeforeDelete = stub.state.memories.length;
  check('doomed row reached upstream before deletion', stub.state.memories.some((m) => m.title === '待删除'));
  const del = await api('POST', '/agentmemory/forget', { memoryIds: [doomed.body.memory.id] });
  eq('delete reports one removal', del.body.removed, 1);
  eq('upstream copy removed', stub.state.memories.length, upCountBeforeDelete - 1);
  await syncStable();
  check('delete does not resurrect after resync', !stub.state.memories.some((m) => m.title === '待删除'));

  console.log('\n7. delete while the write is still queued');
  stub.state.online = false;
  const queued = await api('POST', '/agentmemory/remember', { title: '排队即删', content: '离线写入后马上删除，恢复后不应复活' });
  const beforeQueued = (await api('GET', '/agentmemory/livez')).body.pendingWrites;
  await api('POST', '/agentmemory/forget', { memoryIds: [queued.body.memory.id] });
  const afterQueued = (await api('GET', '/agentmemory/livez')).body.pendingWrites;
  eq('delete also cancels the queued push', afterQueued, beforeQueued - 1);
  stub.state.online = true;
  await syncStable();
  check('cancelled write never arrives upstream', !stub.state.memories.some((m) => m.title === '排队即删'));

  console.log('\n8. export / import');
  const livezBeforeExport = (await api('GET', '/agentmemory/livez')).body;
  const exported = (await api('GET', '/agentmemory/export')).body;
  eq('export carries every local row', exported.memories.length, livezBeforeExport.memories);
  const imported = await api('POST', '/agentmemory/import', {
    memories: [{ id: 'mem_imported_1', title: '导入样本', content: '从快照恢复的记忆', tags: ['import'], scope: 'agent' }],
  });
  eq('import reports one row', imported.body.imported, 1);
  const afterImport = (await api('POST', '/agentmemory/smart-search', { query: '导入样本' })).body;
  check('imported memory is searchable', afterImport.results.some((r) => r.title === '导入样本'));

  console.log('\n9. skills are indexed, read-only and searchable');
  const skillHits = (await api('POST', '/agentmemory/smart-search', { query: 'demo-skill' })).body;
  check('skill and memory share one ranked response', skillHits.results.some((r) => r.layer === 'skill'));
  const reindex = await api('POST', '/hub/skills/reindex');
  eq('reindex scans the directory', reindex.body.skills, 1);
  eq('writing a skill through HTTP is refused', (await api('POST', '/hub/skills')).status, 409);
  const one = await api('GET', '/hub/skills/demo-skill');
  eq('skill lookup returns the latest version', one.body.latest.version, '1.2.0');

  console.log('\n10. snapshots stay fresh');
  await syncStable();
  const snapPath = join(snapDir, 'latest.json');
  check('snapshot written after sync', existsSync(snapPath));
  const snap = JSON.parse(readFileSync(snapPath, 'utf8'));
  eq('snapshot count matches the live row count', snap.count, (await api('GET', '/agentmemory/livez')).body.memories);
  eq('snapshot is marked as local-hub origin', snap.origin, 'local-hub');
} catch (e) {
  check('unexpected error', false, e.message);
  console.error(e);
} finally {
  cleanup();
}

const failed = results.filter((r) => !r.ok);
console.log(`\n${results.length - failed.length}/${results.length} assertions passed`);
if (failed.length) {
  console.log('\nfailures:');
  for (const f of failed) console.log(`  - ${f.name} ${f.detail ? `(${f.detail})` : ''}`);
  process.exit(1);
}
console.log(KEEP ? `temp dir kept at ${work}` : 'temp dir cleaned up');
process.exit(0);
