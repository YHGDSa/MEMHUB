#!/usr/bin/env node
/**
 * Two machines, one upstream.
 *
 * A and B are independent hubs (each with its own database), sharing one
 * passive replica that only speaks the four endpoints a "cloud VM" has to
 * implement: livez / export / remember / forget.
 *
 * The test exercises what incremental sync alone cannot do:
 *   - the same fact recorded on both machines (duplicate)
 *   - a revision made on one machine (last write wins by entity id)
 *   - a deletion that has to stop the other machine from resurrecting it
 *   - full reconciliation repairing a replica that lost a row
 *
 *   node test/multi-hub.mjs
 */
import { createServer } from 'node:http';
import { spawn } from 'node:child_process';
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, existsSync, rmSync, openSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const REPO = join(HERE, '..');
const SECRET = 'multi-hub-secret';
const KEEP = process.argv.includes('--keep');

const rows = [];
function check(name, ok, detail = '') {
  rows.push({ name, ok, detail });
  console.log(`  ${ok ? 'PASS' : 'FAIL'}  ${name}${detail ? `  (${detail})` : ''}`);
}
const eq = (name, actual, expected) => check(name, JSON.stringify(actual) === JSON.stringify(expected), `got ${JSON.stringify(actual)}`);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function freePort() {
  return new Promise((resolve, reject) => {
    const s = createServer(); s.listen(0, '127.0.0.1', () => { const p = s.address().port; s.close(() => resolve(p)); }); s.on('error', reject);
  });
}
async function waitFor(fn, ms = 15000, label = 'condition') {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    try { if (await fn()) return true; } catch { /* keep polling */ }
    await sleep(120);
  }
  throw new Error(`timeout waiting for ${label}`);
}

/* ------------------------------------------------------------ stub upstream */
function startStub(port) {
  const st = { memories: [], seq: 0 };
  const server = createServer(async (req, res) => {
    const send = (code, obj) => { res.writeHead(code, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(obj)); };
    const url = new URL(req.url, 'http://x');
    if (url.pathname === '/agentmemory/livez') return send(200, { status: 'ok' });
    if (req.headers.authorization !== `Bearer ${SECRET}`) return send(401, { error: 'unauthorized' });
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
      const before = st.memories.length;
      st.memories = st.memories.filter((m) => m.id !== body.memoryId);
      return send(200, { success: true, removed: before - st.memories.length });
    }
    return send(404, { error: 'not found' });
  });
  return new Promise((resolve) => server.listen(port, '127.0.0.1', () => resolve({ st, server })));
}

/* ---------------------------------------------------------------- hub setup */
function startHub(name, port, upstream, work) {
  const dir = join(work, name);
  mkdirSync(dir, { recursive: true });
  mkdirSync(join(dir, 'snap'), { recursive: true });
  mkdirSync(join(dir, 'skills'), { recursive: true });
  writeFileSync(join(dir, 'secret'), SECRET + '\n', 'utf8');
  const cfg = {
    port,
    upstream,
    secretFile: join(dir, 'secret'),
    db: 'hub.db',
    syncIntervalMs: 3600000,
    snapDir: './snap',
    skillsDir: join(dir, 'skills'),
  };
  const cfgPath = join(dir, 'config.json');
  writeFileSync(cfgPath, JSON.stringify(cfg, null, 2), 'utf8');
  const env = { ...process.env };
  for (const k of ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']) delete env[k];
  const log = openSync(join(dir, 'hub.log'), 'a');
  const proc = spawn(process.execPath, [join(REPO, 'src', 'hub.mjs'), cfgPath], { cwd: REPO, env, stdio: ['ignore', log, log] });
  return { proc, base: `http://127.0.0.1:${port}` };
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

/* --------------------------------------------------------------------- main */
const work = mkdtempSync(join(tmpdir(), 'multihub-'));
let hubA, hubB, up;
const nonTombstone = () => up.st.memories.filter((m) => m.type !== 'tombstone');
const withContent = (text) => nonTombstone().filter((m) => String(m.content).includes(text));

try {
  const [pa, pb, pu] = [await freePort(), await freePort(), await freePort()];
  up = await startStub(pu);
  hubA = startHub('A', pa, `http://127.0.0.1:${pu}`, work);
  hubB = startHub('B', pb, `http://127.0.0.1:${pu}`, work);
  const A = api(hubA.base);
  const B = api(hubB.base);
  await waitFor(async () => (await A('GET', '/agentmemory/health')).status === 200, 20000, 'hub A');
  await waitFor(async () => (await B('GET', '/agentmemory/health')).status === 200, 20000, 'hub B');

  const syncStable = async (C) => {
    for (let i = 0; i < 40; i++) {
      const r = (await C('POST', '/agentmemory/sync')).body;
      if (!r.skipped) return r;
      await sleep(100);
    }
    throw new Error('sync never left the in-flight state');
  };
  const reconcile = async (C) => (await C('POST', '/hub/reconcile')).body;

  console.log('\n1. replication through a shared replica');
  await A('POST', '/agentmemory/remember', { title: '部署结论', content: '上游放虚拟机上就够了', tags: ['multi'] });
  await syncStable(A);
  eq('A 的写入到达上游', nonTombstone().length, 1);
  await syncStable(B);
  const bFirst = (await B('GET', '/agentmemory/memories?limit=100')).body;
  check('B 未写入也看到了 A 的记录', bFirst.memories.some((m) => m.title === '部署结论'));
  check('B 上这条标为 upstream 来源', bFirst.memories.find((m) => m.title === '部署结论')?.source === 'upstream');

  console.log('\n2. two machines record the same fact independently');
  await A('POST', '/agentmemory/remember', { title: '同一事实', content: '两台机器各记了一遍' });
  await sleep(20);
  await B('POST', '/agentmemory/remember', { title: '同一事实', content: '两台机器各记了一遍' });
  await syncStable(A);
  await syncStable(B);
  await syncStable(A);
  eq('去重前上游有两份', withContent('两台机器各记了一遍').length, 2);
  const rec1 = await reconcile(A);
  eq('对账删掉一份重复', rec1.dupsRemoved, 1);
  eq('上游只剩一份', withContent('两台机器各记了一遍').length, 1);
  await syncStable(A);
  await syncStable(B);
  const aDup = (await A('GET', '/agentmemory/memories?limit=100')).body.memories.filter((m) => m.content.includes('两台机器各记了一遍'));
  const bDup = (await B('GET', '/agentmemory/memories?limit=100')).body.memories.filter((m) => m.content.includes('两台机器各记了一遍'));
  check('A 本地只留一份', aDup.length === 1, `A=${aDup.length}`);
  check('B 本地只留一份', bDup.length === 1, `B=${bDup.length}`);

  console.log('\n3. revision propagates, old version is compacted');
  const target = (await A('GET', '/agentmemory/memories?limit=100')).body.memories.find((m) => m.title === '部署结论');
  const revised = await A('POST', '/agentmemory/revise', { memoryId: target.id, content: '上游放虚拟机上就够了（v2：加一条备份策略）' });
  eq('revise 成功', revised.status, 200);
  await syncStable(A);
  await syncStable(B);
  const bRevised = (await B('GET', '/agentmemory/memories?limit=100')).body.memories.find((m) => m.title === '部署结论');
  check('B 看到的是新版本', String(bRevised?.content).includes('v2'), bRevised?.content);
  eq('B 上没有多出一条平行事实', (await B('GET', '/agentmemory/memories?limit=100')).body.memories.filter((m) => m.title === '部署结论').length, 1);
  const rec2 = await reconcile(A);
  check('对账清掉了旧版本', rec2.versionsRemoved >= 1, `versionsRemoved=${rec2.versionsRemoved}`);
  const eid = (await A('GET', '/agentmemory/memories?limit=100')).body.memories.find((m) => m.title === '部署结论').entityId;
  const sameEntity = nonTombstone().filter((m) => (m.tags || []).includes(`eid:${eid}`));
  eq('上游同一实体只剩一版', sameEntity.length, 1);

  console.log('\n4. deletion stops the other machine from resurrecting it');
  const doomed = (await A('GET', '/agentmemory/memories?limit=100')).body.memories.find((m) => m.title === '部署结论');
  const del = await A('POST', '/agentmemory/forget', { memoryIds: [doomed.id] });
  eq('删除在本地生效并留下墓碑', del.body.tombstones >= 1, true);
  await syncStable(A);
  await syncStable(B);
  const bAfterDel = (await B('GET', '/agentmemory/memories?limit=100')).body.memories;
  check('B 下一次同步后不再有这条', !bAfterDel.some((m) => m.title === '部署结论'));
  const reap = await B('POST', '/agentmemory/remember', { title: '部署结论', content: '上游放虚拟机上就够了（v2：加一条备份策略）' });
  eq('B 重写同一内容被墓碑挡住', reap.status, 409);
  check('上游没有复活该内容', withContent('v2：加一条备份策略').length === 0, `rows=${withContent('v2：加一条备份策略').length}`);

  console.log('\n5. reconciliation repairs a replica that lost a row');
  const survivor = nonTombstone()[0];
  up.st.memories = up.st.memories.filter((m) => m.id !== survivor.id);
  const before = nonTombstone().length;
  const rec3 = await reconcile(A);
  check('对账发现缺失并重推', rec3.repushed >= 1, `repushed=${rec3.repushed}`);
  eq('上游条数恢复', nonTombstone().length, before + 1);
} catch (e) {
  check('unexpected error', false, e.message);
  console.error(e);
} finally {
  try { hubA?.proc.kill(); } catch {}
  try { hubB?.proc.kill(); } catch {}
  try { up?.server.close(); } catch {}
  await sleep(200);
  if (!KEEP) { try { rmSync(work, { recursive: true, force: true }); } catch {} }
}

const failed = rows.filter((r) => !r.ok);
console.log(`\n${rows.length - failed.length}/${rows.length} assertions passed`);
if (failed.length) {
  console.log('\nfailures:');
  for (const f of failed) console.log(`  - ${f.name} ${f.detail ? `(${f.detail})` : ''}`);
  process.exit(1);
}
console.log(KEEP ? `temp dir kept at ${work}` : 'temp dir cleaned up');
process.exit(0);
