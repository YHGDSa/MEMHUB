#!/usr/bin/env node
/**
 * Local-first memory hub.
 *
 * Serves a subset of the AgentMemory REST contract that agents actually use.
 * Writes land here first, so agents keep sharing memory while the upstream is
 * unreachable; an outbox replays them later. The upstream is normally reached
 * through an SSH tunnel on loopback, so the hub never trusts it to be online.
 *
 * Usage: node src/hub.mjs [path/to/config.json]
 */

import { DatabaseSync } from 'node:sqlite';
import { createServer } from 'node:http';
import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync, mkdirSync, existsSync, renameSync, readdirSync, statSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';

const HERE = dirname(new URL(import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, '$1'));
// An explicit config path lets the smoke test run a throwaway hub on a random
// port against a temp database, without touching the real data directory.
const CONFIG_PATH = process.argv[2] || process.env.HUB_CONFIG || join(HERE, '..', 'config.json');
const cfg = JSON.parse(readFileSync(CONFIG_PATH, 'utf8'));
const ROOT = dirname(CONFIG_PATH);

const SECRET = readFileSync(resolve(ROOT, cfg.secretFile), 'utf8').trim();
const DB_PATH = join(ROOT, cfg.db);
// The snapshot is the offline copy of the truth; keep it next to the data
// unless the config says otherwise.
const SNAP_DIR = cfg.snapDir ? resolve(ROOT, cfg.snapDir) : join(ROOT, '..', 'snap');
mkdirSync(SNAP_DIR, { recursive: true });

const db = new DatabaseSync(DB_PATH);
db.exec(`
  PRAGMA journal_mode = WAL;
  CREATE TABLE IF NOT EXISTS memories (
    id           TEXT PRIMARY KEY,
    upstream_id  TEXT,
    title        TEXT NOT NULL,
    content      TEXT NOT NULL,
    type         TEXT NOT NULL DEFAULT 'fact',
    tags         TEXT NOT NULL DEFAULT '[]',
    project      TEXT,
    origin       TEXT NOT NULL DEFAULT 'local-hub',
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL,
    version      INTEGER NOT NULL DEFAULT 1,
    remote       INTEGER NOT NULL DEFAULT 0
  );
  CREATE TABLE IF NOT EXISTS outbox (
    seq        INTEGER PRIMARY KEY AUTOINCREMENT,
    kind       TEXT NOT NULL,
    payload    TEXT NOT NULL,
    created_at TEXT NOT NULL,
    synced_at  TEXT,
    upstream_id TEXT,
    error      TEXT
  );
  CREATE TABLE IF NOT EXISTS skills (
    name        TEXT NOT NULL,
    version     TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    content     TEXT NOT NULL DEFAULT '',
    files       TEXT NOT NULL DEFAULT '[]',
    tags        TEXT NOT NULL DEFAULT '[]',
    created_at  TEXT NOT NULL,
    PRIMARY KEY (name, version)
  );
  CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
  -- Deletion markers. A tombstone is local state, not a memory: it never shows
  -- up in search or in the snapshot, but it stops a deleted fact from being
  -- resurrected by another machine's copy.
  CREATE TABLE IF NOT EXISTS tombstones (
    cid        TEXT PRIMARY KEY,
    entity_id  TEXT,
    deleted_at TEXT NOT NULL,
    origin     TEXT
  );
  CREATE INDEX IF NOT EXISTS idx_mem_updated ON memories(updated_at);
`);

// Older databases predate these columns; add them in place instead of asking
// users to migrate by hand.
const columns = new Set(db.prepare('PRAGMA table_info(memories)').all().map((c) => c.name));
for (const [name, ddl] of [
  ['files', "ALTER TABLE memories ADD COLUMN files TEXT NOT NULL DEFAULT '[]'"],
  ['scope', "ALTER TABLE memories ADD COLUMN scope TEXT NOT NULL DEFAULT 'global'"],
  ['content_id', 'ALTER TABLE memories ADD COLUMN content_id TEXT'],
  ['entity_id', 'ALTER TABLE memories ADD COLUMN entity_id TEXT'],
]) {
  if (!columns.has(name)) db.exec(ddl);
}
db.exec('CREATE INDEX IF NOT EXISTS idx_mem_cid ON memories(content_id)');
db.exec('CREATE INDEX IF NOT EXISTS idx_mem_eid ON memories(entity_id)');

const nowIso = () => new Date().toISOString();
const j = (v) => JSON.stringify(v ?? null);
// `private` never leaves this machine: no outbox row, so it cannot be pushed.
const SCOPES = new Set(['global', 'project', 'agent', 'private']);

/* ---------- identity ---------- */
// Two machines may record the same fact independently. A content id derived
// from the normalised record lets both ends recognise it as one thing without
// asking the upstream to deduplicate anything.
const TOMBSTONE_TYPE = 'tombstone';
const CID_TAG = 'cid:';
const EID_TAG = 'eid:';
const TOMB_TAG = 'tomb:';
const TOMBSTONE_TTL_DAYS = Number(cfg.tombstoneTtlDays ?? 30);
const TOMBSTONES_ON = cfg.tombstones !== false;

const norm = (s) => String(s ?? '').normalize('NFKC').replace(/\s+/g, ' ').trim().toLowerCase();
function cidOf({ title, content, project, type } = {}) {
  return createHash('sha256')
    .update([norm(project), norm(type || 'fact'), norm(title), norm(content)].join('\u0000'))
    .digest('hex')
    .slice(0, 16);
}

function tagValue(tags, prefix) {
  const hit = (Array.isArray(tags) ? tags : []).find((t) => String(t).startsWith(prefix));
  return hit ? String(hit).slice(prefix.length) : null;
}

// Metadata travels in tags, so it survives any upstream that only stores
// tags/concepts -- no new endpoint or field is required from the replica.
function metaOf(m) {
  const tags = Array.isArray(m.tags) ? m.tags : (Array.isArray(m.concepts) ? m.concepts : []);
  const isTombstone = m.type === TOMBSTONE_TYPE || tags.some((t) => String(t).startsWith(TOMB_TAG));
  const cid = m.contentId || m.content_id || tagValue(tags, CID_TAG) || tagValue(tags, TOMB_TAG)
    || (isTombstone ? null : cidOf(m));
  const eid = m.entityId || m.entity_id || tagValue(tags, EID_TAG) || m.id || null;
  return { tags, isTombstone, cid, eid };
}

function mergeTags(base, cid, eid) {
  const out = (Array.isArray(base) ? base : [])
    .filter((t) => !String(t).startsWith(CID_TAG) && !String(t).startsWith(EID_TAG) && !String(t).startsWith(TOMB_TAG));
  if (cid) out.push(CID_TAG + cid);
  if (eid) out.push(EID_TAG + eid);
  return out;
}

// A tombstone suppresses the same content on every replica for a bounded time;
// after the TTL the fact can be recorded again on purpose.
function liveTombstones() {
  const cutoff = new Date(Date.now() - TOMBSTONE_TTL_DAYS * 86400000).toISOString();
  return new Set(db.prepare('SELECT cid FROM tombstones WHERE deleted_at > ?').all(cutoff).map((r) => r.cid));
}

function addTombstone(cid, entityId, when, origin) {
  if (!cid) return;
  db.prepare('INSERT OR REPLACE INTO tombstones(cid, entity_id, deleted_at, origin) VALUES(?, ?, ?, ?)')
    .run(cid, entityId || null, when || nowIso(), origin || 'local');
}

// Rows written before content ids existed still need an identity, otherwise
// cross-machine dedup silently skips them.
for (const r of db.prepare('SELECT id, title, content, project, type, tags, content_id, entity_id FROM memories WHERE content_id IS NULL OR entity_id IS NULL').all()) {
  const cid = r.content_id || cidOf(r);
  const eid = r.entity_id || r.id;
  db.prepare('UPDATE memories SET content_id = ?, entity_id = ?, tags = ? WHERE id = ?')
    .run(cid, eid, j(mergeTags(JSON.parse(r.tags || '[]'), cid, eid)), r.id);
}

function metaGet(k, dflt = null) {
  const r = db.prepare('SELECT value FROM meta WHERE key = ?').get(k);
  return r ? r.value : dflt;
}
function metaSet(k, v) {
  db.prepare('INSERT INTO meta(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value').run(k, String(v));
}

/* ---------- redaction ---------- */
// Runs before anything is stored, so a secret pasted into a memory cannot reach
// the NAS, the snapshot or a backup. Patterns are deliberately broad; a false
// positive only costs readability.
const SECRET_PATTERNS = [
  [/-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----/g, '[REDACTED PRIVATE KEY]'],
  [/\b(sk-[A-Za-z0-9_-]{12,}|ghp_[A-Za-z0-9]{20,}|gho_[A-Za-z0-9]{20,}|ghs_[A-Za-z0-9]{20,}|xox[baprs]-[A-Za-z0-9-]{10,}|AKIA[0-9A-Z]{16})\b/g, '[REDACTED]'],
  [/(Bearer\s+)[A-Za-z0-9._~+/-]{12,}=*/gi, '$1[REDACTED]'],
  [/(\/\/)[^/\s:@]+:[^/\s@]+@/g, '$1[REDACTED]:[REDACTED]@'],
  [/("(?:api[_-]?key|token|secret|password)"\s*:\s*")[^"]{6,}(")/gi, '$1[REDACTED]$2'],
];

function redact(text) {
  let out = String(text ?? '');
  let hits = 0;
  for (const [re, repl] of SECRET_PATTERNS) {
    out = out.replace(re, (...args) => { hits++; return typeof repl === 'function' ? repl(...args) : repl.replace(/\$(\d)/g, (_, i) => args[i]); });
  }
  return { text: out, hits };
}

let upstreamState = { online: false, checkedAt: null, lastError: null, lastPushAt: null, lastPullAt: null };

async function upstreamFetch(path, init = {}, timeoutMs = 8000) {
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), timeoutMs);
  try {
    const res = await fetch(cfg.upstream + path, {
      ...init,
      signal: ctl.signal,
      headers: { Authorization: `Bearer ${SECRET}`, 'Content-Type': 'application/json', ...(init.headers || {}) },
    });
    const text = await res.text();
    let body = null;
    try { body = text ? JSON.parse(text) : null; } catch { body = { raw: text }; }
    return { ok: res.ok, status: res.status, body };
  } finally {
    clearTimeout(timer);
  }
}

function upsertMemory(m) {
  const tags = Array.isArray(m.tags) ? m.tags : (Array.isArray(m.concepts) ? m.concepts : []);
  const files = Array.isArray(m.files) ? m.files : [];
  const scope = SCOPES.has(m.scope) ? m.scope : 'global';
  const origin = typeof m.origin === 'object' && m.origin ? (m.origin.channel || 'remote') : (m.origin || 'remote');
  const meta = metaOf(m);
  const stmt = db.prepare(`
    INSERT INTO memories (id, upstream_id, title, content, type, tags, project, files, scope, origin, created_at, updated_at, version, remote, content_id, entity_id)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      title = excluded.title, content = excluded.content, type = excluded.type,
      tags = excluded.tags, project = excluded.project, files = excluded.files, scope = excluded.scope,
      updated_at = excluded.updated_at, content_id = excluded.content_id, entity_id = excluded.entity_id,
      version = excluded.version, upstream_id = excluded.upstream_id, remote = excluded.remote
    WHERE excluded.updated_at >= memories.updated_at
  `);
  stmt.run(
    m.id, m.upstream_id ?? (m.remote ? m.id : null), m.title || '(untitled)', m.content || '',
    m.type || 'fact', j(tags), m.project ?? null, j(files), scope, origin,
    m.createdAt || m.created_at || nowIso(), m.updatedAt || m.updated_at || nowIso(),
    Number(m.version || 1), m.remote ? 1 : 0, meta.cid, meta.eid,
  );
}

function rowToMemory(r) {
  return {
    id: r.id,
    title: r.title,
    content: r.content,
    type: r.type,
    concepts: JSON.parse(r.tags || '[]'),
    tags: JSON.parse(r.tags || '[]'),
    project: r.project ?? undefined,
    files: JSON.parse(r.files || '[]'),
    scope: r.scope || 'global',
    origin: { channel: r.origin, capturedAt: r.created_at },
    createdAt: r.created_at,
    updatedAt: r.updated_at,
    version: r.version,
    isLatest: true,
    source: r.remote ? 'upstream' : 'local',
    contentId: r.content_id || undefined,
    entityId: r.entity_id || undefined,
  };
}

/* ---------- search ---------- */

function searchMemories(query, limit) {
  const terms = String(query || '').toLowerCase().split(/\s+/).filter(Boolean);
  const rows = db.prepare('SELECT * FROM memories ORDER BY updated_at DESC LIMIT 5000').all();
  const now = Date.now();
  const scored = [];
  const score = (termsList, fields) => {
    let hits = 0;
    for (const t of termsList) {
      if (fields.title.includes(t)) hits += 3;
      else if (fields.tags.includes(t)) hits += 2;
      else if (fields.body.includes(t)) hits += 1;
    }
    return hits;
  };
  const finish = (hits, updatedAt) => {
    const ageDays = Math.max(0, (now - Date.parse(updatedAt)) / 86400000);
    const recency = 1 / (1 + ageDays / 30);
    const raw = terms.length ? hits / (terms.length * 3) : 0;
    return Math.min(1, raw * 0.85 + recency * 0.15);
  };

  for (const r of rows) {
    const hits = score(terms, { title: r.title.toLowerCase(), tags: (r.tags || '').toLowerCase(), body: r.content.toLowerCase() });
    if (terms.length && hits === 0) continue;
    scored.push({
      layer: 'memory',
      obsId: r.id,
      memoryId: r.id,
      sessionId: 'memory',
      timestamp: r.updated_at,
      title: r.title,
      type: r.type,
      // Provenance must travel with the hit: without it a second agent cannot
      // tell whose memory it just read, or whether it may quote it onwards.
      scope: r.scope || 'global',
      tags: JSON.parse(r.tags || '[]'),
      project: r.project || null,
      files: JSON.parse(r.files || '[]'),
      score: Number(finish(hits, r.updated_at).toFixed(4)),
      content: r.content.slice(0, 400),
    });
  }

  if (terms.length) {
    const skills = db.prepare(`
      SELECT s.* FROM skills s
      JOIN (SELECT name, MAX(created_at) AS mx FROM skills GROUP BY name) t
        ON s.name = t.name AND s.created_at = t.mx
    `).all();
    for (const s of skills) {
      const hits = score(terms, {
        title: s.name.toLowerCase(),
        tags: (s.tags || '').toLowerCase(),
        body: `${s.description}\n${s.content}`.toLowerCase(),
      });
      if (hits === 0) continue;
      scored.push({
        layer: 'skill',
        obsId: `skill:${s.name}@${s.version}`,
        sessionId: 'skills',
        timestamp: s.created_at,
        title: `${s.name}@${s.version}`,
        type: 'skill',
        score: Number(finish(hits, s.created_at).toFixed(4)),
        content: s.description,
      });
    }
  }

  scored.sort((a, b) => b.score - a.score || b.timestamp.localeCompare(a.timestamp));
  return scored.slice(0, Math.max(1, Math.min(50, limit || 5)));
}

/* ---------- sync ---------- */

let syncing = false;

function writeSnapshot() {
  const skills = db.prepare('SELECT name, version, description, content, files, tags, created_at FROM skills').all();
  const snapshot = {
    exportedAt: nowIso(),
    origin: 'local-hub',
    count: db.prepare('SELECT COUNT(*) c FROM memories').get().c,
    memories: db.prepare('SELECT * FROM memories ORDER BY created_at ASC').all().map(rowToMemory),
    skills: skills.map((s) => ({ ...s, files: JSON.parse(s.files), tags: JSON.parse(s.tags) })),
  };
  const tmp = join(SNAP_DIR, '.latest.json.tmp');
  try {
    writeFileSync(tmp, JSON.stringify(snapshot, null, 2), 'utf8');
    renameSync(tmp, join(SNAP_DIR, 'latest.json'));
  } catch (error) {
    // SQLite is already committed. A backup write failure must not stop replay
    // or make a successful mutation look as though it needs to be retried.
    console.error('[hub] snapshot failed', error.message);
  }
}

async function pullUpstream() {
  const res = await upstreamFetch('/agentmemory/export', { method: 'GET' }, 180000);
  if (!res.ok) throw new Error(`export ${res.status}`);
  const memories = res.body?.memories || [];
  const tomb = liveTombstones();
  const knownUpstream = new Set(
    db.prepare('SELECT upstream_id FROM memories WHERE upstream_id IS NOT NULL').all().map((r) => r.upstream_id),
  );
  let added = 0;
  let merged = 0;
  let suppressed = 0;
  let cleaned = 0;

  for (const m of memories) {
    if (!m?.id) continue;
    const meta = metaOf(m);

    // 1) tombstones: record the deletion and drop any local copy of that content
    if (meta.isTombstone) {
      if (meta.cid) {
        const already = db.prepare('SELECT deleted_at FROM tombstones WHERE cid = ?').get(meta.cid);
        if (!already) {
          addTombstone(meta.cid, meta.eid, m.updatedAt || m.updated_at || nowIso(), 'upstream');
          tomb.add(meta.cid);
        }
        for (const row of db.prepare('SELECT id FROM memories WHERE content_id = ?').all(meta.cid)) {
          db.prepare('DELETE FROM memories WHERE id = ?').run(row.id);
          suppressed++;
        }
      }
      continue;
    }

    // 2) our own pushed row: already local, nothing to do
    if (knownUpstream.has(m.id) || db.prepare('SELECT id FROM memories WHERE upstream_id = ?').get(m.id)) continue;

    // 3) content deleted elsewhere: do not insert, and clean the stale upstream copy
    if (meta.cid && tomb.has(meta.cid)) {
      await upstreamFetch('/agentmemory/forget', { method: 'POST', body: JSON.stringify({ memoryId: m.id }) }, 60000).catch(() => {});
      cleaned++;
      continue;
    }

    // 4) same content already present under another id: keep one row
    const byCid = db.prepare('SELECT * FROM memories WHERE content_id = ?').get(meta.cid);
    if (byCid) {
      if (String(m.updatedAt || '') > String(byCid.updated_at)) {
        db.prepare('UPDATE memories SET updated_at = ? WHERE id = ?').run(m.updatedAt, byCid.id);
        merged++;
      }
      continue;
    }

    // 5) newer version of the same logical memory (revision): last write wins
    const byEid = db.prepare('SELECT * FROM memories WHERE entity_id = ? ORDER BY updated_at DESC').get(meta.eid);
    if (byEid) {
      if (String(m.updatedAt || '') > String(byEid.updated_at)) {
        db.prepare('UPDATE memories SET title = ?, content = ?, content_id = ?, updated_at = ?, version = version + 1 WHERE id = ?')
          .run(m.title || byEid.title, m.content || '', meta.cid, m.updatedAt || nowIso(), byEid.id);
        merged++;
      }
      continue;
    }

    // 6) genuinely new to this machine
    const before = db.prepare('SELECT updated_at FROM memories WHERE id = ?').get(m.id);
    upsertMemory({ ...m, remote: true, content_id: meta.cid, entity_id: meta.eid });
    if (!before) added++;
  }

  writeSnapshot();
  return { pulled: memories.length, added, merged, suppressed, cleaned };
}

async function pushOutbox() {
  const rows = db.prepare('SELECT * FROM outbox WHERE synced_at IS NULL ORDER BY seq ASC LIMIT 100').all();
  let pushed = 0;
  for (const row of rows) {
    const payload = JSON.parse(row.payload);
    let body;
    if (row.kind === 'tombstone') {
      // A tombstone is a normal record carrying the deleted content id, so any
      // replica that only speaks remember/export/forget can still carry it.
      body = {
        title: `tombstone:${payload.cid}`,
        content: '[deleted]',
        type: TOMBSTONE_TYPE,
        tags: [`${TOMB_TAG}${payload.cid}`, `${CID_TAG}${payload.cid}`, `${EID_TAG}${payload.entityId || ''}`],
        concepts: [`${TOMB_TAG}${payload.cid}`, `${CID_TAG}${payload.cid}`],
      };
      if (payload.project) body.project = payload.project;
    } else {
      body = { title: payload.title, content: payload.content };
      // Upstream stores keywords as `concepts`; sending only `tags` silently loses them.
      if (payload.tags?.length) body.tags = payload.tags;
      body.concepts = payload.tags || [];
      if (payload.project) body.project = payload.project;
      if (payload.type) body.type = payload.type;
      if (payload.files?.length) body.files = payload.files;
    }
    const res = await upstreamFetch('/agentmemory/remember', {
      method: 'POST',
      body: JSON.stringify(body),
    }, 60000);
    if (!res.ok || !res.body?.success) {
      db.prepare('UPDATE outbox SET error = ? WHERE seq = ?').run(`HTTP ${res.status}`, row.seq);
      continue;
    }
    const upstreamId = res.body?.memory?.id || null;
    db.prepare('UPDATE outbox SET synced_at = ?, upstream_id = ?, error = NULL WHERE seq = ?').run(nowIso(), upstreamId, row.seq);
    if (payload.memoryId) {
      db.prepare('UPDATE memories SET upstream_id = ? WHERE id = ?').run(upstreamId, payload.memoryId);
    }
    pushed++;
  }
  return { pending: rows.length, pushed };
}

async function syncNow(reason = 'timer') {
  if (syncing) return { skipped: 'already-running' };
  syncing = true;
  const report = { at: nowIso(), reason, upstream: false };
  try {
    const health = await upstreamFetch('/agentmemory/livez', { method: 'GET' }, 30000);
    upstreamState.online = health.ok;
    upstreamState.checkedAt = nowIso();
    if (!health.ok) throw new Error(`livez ${health.status}`);
    report.upstream = true;
    report.pull = await pullUpstream();
    upstreamState.lastPullAt = nowIso();
    report.push = await pushOutbox();
    upstreamState.lastPushAt = nowIso();
    upstreamState.lastError = null;
    metaSet('last_sync_ok', nowIso());
  } catch (e) {
    upstreamState.online = false;
    upstreamState.lastError = String(e.message || e);
    report.error = upstreamState.lastError;
  } finally {
    syncing = false;
  }
  metaSet('last_sync_report', JSON.stringify(report));
  return report;
}

/* ---------- http ---------- */

const routes = [];
const route = (method, path, handler) => routes.push({ method, path, handler });

function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', (c) => { data += c; if (data.length > 2e6) reject(new Error('body too large')); });
    req.on('end', () => { try { resolve(data ? JSON.parse(data) : {}); } catch (e) { reject(e); } });
    req.on('error', reject);
  });
}

route('GET', '/agentmemory/livez', () => ({
  service: 'agentmemory',
  status: 'ok',
  mode: 'local-hub',
  upstream: upstreamState.online ? 'online' : 'offline',
  memories: db.prepare('SELECT COUNT(*) c FROM memories').get().c,
  pendingWrites: db.prepare('SELECT COUNT(*) c FROM outbox WHERE synced_at IS NULL').get().c,
  tombstones: db.prepare('SELECT COUNT(*) c FROM tombstones').get().c,
}));

route('GET', '/agentmemory/health', () => ({
  service: 'agentmemory',
  status: 'ok',
  mode: 'local-hub',
  upstream: upstreamState,
  lastSyncOk: metaGet('last_sync_ok'),
  memories: db.prepare('SELECT COUNT(*) c FROM memories').get().c,
  tombstones: db.prepare('SELECT COUNT(*) c FROM tombstones').get().c,
  skills: db.prepare('SELECT COUNT(DISTINCT name) c FROM skills').get().c,
}));

route('POST', '/agentmemory/remember', (body) => {
  const ts = nowIso();
  const id = `mem_${Date.now().toString(36)}_${Math.random().toString(16).slice(2, 12)}`;
  const baseTags = Array.isArray(body.tags) ? body.tags : (Array.isArray(body.concepts) ? body.concepts : []);
  const scope = body.scope ?? 'global';
  if (!SCOPES.has(scope)) throw Object.assign(new Error(`unknown scope: ${scope}`), { status: 400 });
  const title = redact(body.title || (body.content || '').slice(0, 80) || '(untitled)');
  const content = redact(body.content || '');
  const type = body.type || 'fact';
  const project = body.project ?? null;
  const cid = cidOf({ title: title.text, content: content.text, project, type });
  const entityId = body.entityId || `e_${createHash('sha256').update(`${id}`).digest('hex').slice(0, 12)}`;

  // A deleted fact stays deleted on every replica for the tombstone TTL, even
  // if another agent writes it again by accident.
  if (scope !== 'private' && TOMBSTONES_ON && liveTombstones().has(cid)) {
    throw Object.assign(new Error('tombstoned: this content was deleted; wait for the tombstone TTL or write it as a new fact'), { status: 409 });
  }

  const tags = mergeTags(baseTags, cid, entityId);
  const memory = {
    id,
    upstream_id: null,
    title: title.text,
    content: content.text,
    type,
    tags,
    project,
    files: Array.isArray(body.files) ? body.files : [],
    scope,
    origin: 'local-hub',
    createdAt: ts,
    updatedAt: ts,
    version: 1,
    remote: 0,
    content_id: cid,
    entity_id: entityId,
  };
  upsertMemory(memory);
  if (scope !== 'private') {
    db.prepare('INSERT INTO outbox(kind, payload, created_at) VALUES(?, ?, ?)').run(
      'remember',
      j({ memoryId: id, title: memory.title, content: memory.content, tags, files: memory.files, project: memory.project, type: memory.type }),
      ts,
    );
  }
  const saved = db.prepare('SELECT * FROM memories WHERE id = ?').get(id);
  if (scope !== 'private' && upstreamState.online) syncNow('write').catch(() => {});
  return { success: true, redacted: title.hits + content.hits, memory: rowToMemory(saved) };
});

// Revisions keep the same entity id, so other machines resolve them by
// last-write-wins instead of storing a second, unrelated fact.
route('POST', '/agentmemory/revise', (body) => {
  const id = body.memoryId || body.id;
  const row = id ? db.prepare('SELECT * FROM memories WHERE id = ?').get(id) : null;
  if (!row) throw Object.assign(new Error('memory not found'), { status: 404 });
  const title = redact(body.title ?? row.title);
  const content = redact(body.content ?? row.content);
  const ts = nowIso();
  const cid = cidOf({ title: title.text, content: content.text, project: row.project, type: row.type });
  const entityId = row.entity_id || row.id;
  const tags = mergeTags(JSON.parse(row.tags || '[]'), cid, entityId);
  db.prepare('UPDATE memories SET title = ?, content = ?, content_id = ?, tags = ?, updated_at = ?, version = version + 1 WHERE id = ?')
    .run(title.text, content.text, cid, j(tags), ts, id);
  if (row.scope !== 'private') {
    db.prepare('INSERT INTO outbox(kind, payload, created_at) VALUES(?, ?, ?)').run(
      'remember',
      j({ memoryId: id, title: title.text, content: content.text, tags, files: JSON.parse(row.files || '[]'), project: row.project, type: row.type }),
      ts,
    );
    if (upstreamState.online) syncNow('revise').catch(() => {});
  }
  const saved = db.prepare('SELECT * FROM memories WHERE id = ?').get(id);
  return { success: true, redacted: title.hits + content.hits, memory: rowToMemory(saved) };
});

route('POST', '/agentmemory/smart-search', (body) => ({
  mode: 'compact',
  results: searchMemories(body.query, body.limit),
  lessons: [],
}));

route('GET', '/agentmemory/memories', (body, url) => {
  const limit = Math.max(1, Math.min(500, Number(url.searchParams.get('limit') || 50)));
  const rows = db.prepare('SELECT * FROM memories ORDER BY updated_at DESC LIMIT ?').all(limit);
  return { limit, total: db.prepare('SELECT COUNT(*) c FROM memories').get().c, memories: rows.map(rowToMemory) };
});

route('GET', '/agentmemory/export', () => {
  const rows = db.prepare('SELECT * FROM memories ORDER BY created_at ASC').all();
  return {
    version: 'hub-1',
    exportedAt: nowIso(),
    memories: rows.map(rowToMemory),
    skills: db.prepare('SELECT * FROM skills').all().map((s) => ({ ...s, files: JSON.parse(s.files), tags: JSON.parse(s.tags) })),
  };
});

route('POST', '/agentmemory/import', (body) => {
  const list = body.memories || [];
  let n = 0;
  for (const m of list) { upsertMemory({ ...m, remote: m.remote ?? false }); n++; }
  // A bulk import is exactly when the offline copy matters most (migration,
  // restore); refresh it instead of waiting for the next sync or the 5 min timer.
  if (n) writeSnapshot();
  return { success: true, imported: n };
});

route('POST', '/agentmemory/forget', async (body) => {
  const ids = body.memoryIds || (body.memoryId ? [body.memoryId] : []);
  let removed = 0;
  let tombstones = 0;
  for (const id of ids) {
    const row = db.prepare('SELECT * FROM memories WHERE id = ?').get(id);
    const ts = nowIso();
    if (row) {
      const cid = row.content_id || cidOf({ title: row.title, content: row.content, project: row.project, type: row.type });
      if (cid) {
        addTombstone(cid, row.entity_id || id, ts, 'local');
        tombstones++;
        // Tell the replicas that this content is gone. A tombstone is a regular
        // record, so it travels through the same remember/export path.
        if (TOMBSTONES_ON && row.scope !== 'private') {
          db.prepare('INSERT INTO outbox(kind, payload, created_at) VALUES(?, ?, ?)').run(
            'tombstone',
            j({ cid, entityId: row.entity_id || id, project: row.project ?? null }),
            ts,
          );
        }
      }
      db.prepare('DELETE FROM memories WHERE id = ?').run(id);
      // Drop any queued write for this id, otherwise the next replay recreates
      // it upstream -- deleting while the upstream is offline would silently undo
      // itself minutes later.
      for (const o of db.prepare("SELECT seq, payload FROM outbox WHERE kind = 'remember'").all()) {
        let mid = null;
        try {
          mid = JSON.parse(o.payload).memoryId;
        } catch {
          mid = null;
        }
        if (mid === id) db.prepare('DELETE FROM outbox WHERE seq = ?').run(o.seq);
      }
      removed++;
      if (row.upstream_id) {
        await upstreamFetch('/agentmemory/forget', { method: 'POST', body: JSON.stringify({ memoryId: row.upstream_id }) }, 60000).catch(() => {});
      }
    }
  }
  writeSnapshot();
  return { success: true, removed, tombstones };
});

// Full reconciliation: compare this machine with the whole upstream set and
// repair what incremental sync cannot see -- duplicates created by two machines
// recording the same fact, stale versions left by a revision, and rows that
// vanished upstream while nobody was looking.
async function reconcile() {
  const startedAt = nowIso();
  const report = { at: startedAt, upstream: false, pulled: 0, added: 0, merged: 0, suppressed: 0, cleaned: 0, dupsRemoved: 0, versionsRemoved: 0, localDupsRemoved: 0, repushed: 0, tombstonesCompacted: 0 };
  const health = await upstreamFetch('/agentmemory/livez', { method: 'GET' }, 30000);
  if (!health.ok) throw new Error(`upstream unreachable: ${health.status}`);
  report.upstream = true;

  const pulled = await pullUpstream();
  Object.assign(report, { pulled: pulled.pulled, added: pulled.added, merged: pulled.merged, suppressed: pulled.suppressed, cleaned: pulled.cleaned });

  const res = await upstreamFetch('/agentmemory/export', { method: 'GET' }, 180000);
  if (!res.ok) throw new Error(`export ${res.status}`);
  const memories = res.body?.memories || [];
  const tomb = liveTombstones();

  // 1) upstream duplicates: one cid, several rows -> keep the newest, forget the rest
  const groups = new Map();
  const tombstoneRows = [];
  for (const m of memories) {
    if (!m?.id) continue;
    const meta = metaOf(m);
    if (meta.isTombstone) { tombstoneRows.push({ m, meta }); continue; }
    if (meta.cid && tomb.has(meta.cid)) {
      const r = await upstreamFetch('/agentmemory/forget', { method: 'POST', body: JSON.stringify({ memoryId: m.id }) }, 60000).catch(() => null);
      if (r?.ok) report.cleaned++;
      continue;
    }
    if (!groups.has(meta.cid)) groups.set(meta.cid, []);
    groups.get(meta.cid).push(m);
  }
  for (const list of groups.values()) {
    if (list.length < 2) continue;
    list.sort((a, b) => String(b.updatedAt || b.updated_at || '').localeCompare(String(a.updatedAt || a.updated_at || '')));
    for (const loser of list.slice(1)) {
      const r = await upstreamFetch('/agentmemory/forget', { method: 'POST', body: JSON.stringify({ memoryId: loser.id }) }, 60000).catch(() => null);
      if (r?.ok) report.dupsRemoved++;
    }
  }

  // 1b) stale versions of the same logical memory: only the newest one belongs
  //     on the replica, otherwise every revision permanently grows the export
  const live = memories.filter((m) => m?.id && !metaOf(m).isTombstone && !tomb.has(metaOf(m).cid));
  const versions = new Map();
  for (const m of live) {
    const eid = metaOf(m).eid;
    if (!eid) continue;
    if (!versions.has(eid)) versions.set(eid, []);
    versions.get(eid).push(m);
  }
  for (const list of versions.values()) {
    if (list.length < 2) continue;
    list.sort((a, b) => String(b.updatedAt || b.updated_at || '').localeCompare(String(a.updatedAt || a.updated_at || '')));
    for (const loser of list.slice(1)) {
      const r = await upstreamFetch('/agentmemory/forget', { method: 'POST', body: JSON.stringify({ memoryId: loser.id }) }, 60000).catch(() => null);
      if (r?.ok) report.versionsRemoved++;
    }
  }

  // 2) local duplicates (same cid twice on this machine)
  for (const d of db.prepare('SELECT content_id, COUNT(*) c FROM memories WHERE content_id IS NOT NULL GROUP BY content_id HAVING c > 1').all()) {
    const rows = db.prepare('SELECT id FROM memories WHERE content_id = ? ORDER BY updated_at DESC').all(d.content_id);
    for (const r of rows.slice(1)) {
      db.prepare('DELETE FROM memories WHERE id = ?').run(r.id);
      report.localDupsRemoved++;
    }
  }

  // 3) self-heal: a row we once pushed but which is gone upstream and is not
  //    represented there by the same content id -> push it again
  const upstreamIds = new Set(memories.map((m) => m.id));
  const upstreamCids = new Set(memories.filter((m) => !metaOf(m).isTombstone).map((m) => metaOf(m).cid));
  for (const r of db.prepare('SELECT * FROM memories WHERE upstream_id IS NOT NULL AND remote = 0').all()) {
    if (upstreamIds.has(r.upstream_id)) continue;
    if (r.content_id && upstreamCids.has(r.content_id)) continue;
    if (r.content_id && tomb.has(r.content_id)) continue;
    db.prepare('INSERT INTO outbox(kind, payload, created_at) VALUES(?, ?, ?)').run(
      'remember',
      j({ memoryId: r.id, title: r.title, content: r.content, tags: JSON.parse(r.tags || '[]'), files: JSON.parse(r.files || '[]'), project: r.project, type: r.type }),
      nowIso(),
    );
    report.repushed++;
  }
  if (report.repushed) Object.assign(report, { push: await pushOutbox() });

  // 4) compact expired tombstones (TTL passed): drop local marker, forget the
  //    upstream tombstone record so export does not grow forever
  const cutoff = new Date(Date.now() - TOMBSTONE_TTL_DAYS * 86400000).toISOString();
  for (const t of db.prepare('SELECT cid FROM tombstones WHERE deleted_at <= ?').all(cutoff)) {
    db.prepare('DELETE FROM tombstones WHERE cid = ?').run(t.cid);
    report.tombstonesCompacted++;
  }
  for (const { m, meta } of tombstoneRows) {
    if (!meta.cid) continue;
    const local = db.prepare('SELECT deleted_at FROM tombstones WHERE cid = ?').get(meta.cid);
    if (local && local.deleted_at <= cutoff) {
      const r = await upstreamFetch('/agentmemory/forget', { method: 'POST', body: JSON.stringify({ memoryId: m.id }) }, 60000).catch(() => null);
      if (r?.ok) report.tombstonesCompacted++;
    }
  }

  writeSnapshot();
  metaSet('last_reconcile', JSON.stringify(report));
  return report;
}

route('POST', '/hub/reconcile', async () => reconcile());

route('POST', '/agentmemory/sync', async () => syncNow('manual'));

route('GET', '/hub/status', () => ({
  upstream: upstreamState,
  lastSyncOk: metaGet('last_sync_ok'),
  lastSyncReport: JSON.parse(metaGet('last_sync_report', 'null') || 'null'),
  db: DB_PATH,
  snapshot: join(SNAP_DIR, 'latest.json'),
}));

/* skills: the directory on disk is the only writer; this table is a generated
   index so smart-search can rank skills next to memories. */

const SKILLS_DIR = cfg.skillsDir || join(ROOT, '..', 'skills-src');

function parseFrontmatter(text) {
  const m = /^---\r?\n([\s\S]*?)\r?\n---\r?\n?([\s\S]*)$/.exec(text);
  if (!m) return { meta: {}, body: text };
  const meta = {};
  for (const line of m[1].split(/\r?\n/)) {
    const kv = /^([A-Za-z0-9_-]+)\s*:\s*(.*)$/.exec(line);
    if (kv) meta[kv[1].toLowerCase()] = kv[2].trim().replace(/^["']|["']$/g, '');
  }
  return { meta, body: m[2] };
}

function reindexSkills() {
  if (!existsSync(SKILLS_DIR)) return { skills: 0, dir: SKILLS_DIR, missing: true };
  const found = [];
  for (const entry of readdirSync(SKILLS_DIR)) {
    const dir = join(SKILLS_DIR, entry);
    const file = join(dir, 'SKILL.md');
    if (!statSync(dir).isDirectory() || !existsSync(file)) continue;
    const { meta, body } = parseFrontmatter(readFileSync(file, 'utf8'));
    found.push({
      name: meta.name || entry,
      version: meta.version || '0.0.0',
      description: meta.description || '',
      content: body.trim(),
      files: [file],
      tags: (meta.tags || '').split(',').map((t) => t.trim()).filter(Boolean),
      created_at: nowIso(),
    });
  }
  db.exec('BEGIN');
  try {
    db.exec('DELETE FROM skills');
    const ins = db.prepare('INSERT INTO skills(name, version, description, content, files, tags, created_at) VALUES(?, ?, ?, ?, ?, ?, ?)');
    for (const s of found) ins.run(s.name, s.version, s.description, s.content, j(s.files), j(s.tags), s.created_at);
    db.exec('COMMIT');
  } catch (e) {
    db.exec('ROLLBACK');
    throw e;
  }
  writeFileSync(join(SNAP_DIR, 'skills.json'), JSON.stringify(found, null, 2), 'utf8');
  return { skills: found.length, dir: SKILLS_DIR, names: found.map((s) => `${s.name}@${s.version}`) };
}

route('POST', '/hub/skills/reindex', () => ({ success: true, ...reindexSkills() }));

route('GET', '/hub/skills', () => {
  const rows = db.prepare(`
    SELECT s.* FROM skills s
    JOIN (SELECT name, MAX(created_at) AS mx FROM skills GROUP BY name) t
      ON s.name = t.name AND s.created_at = t.mx
    ORDER BY s.name
  `).all();
  return { count: rows.length, dir: SKILLS_DIR, skills: rows.map((s) => ({ ...s, files: JSON.parse(s.files), tags: JSON.parse(s.tags) })) };
});

route('POST', '/hub/skills', () => {
  throw Object.assign(new Error(
    `skills are read-only here: add <name>/SKILL.md under ${SKILLS_DIR}, then POST /hub/skills/reindex`,
  ), { status: 409 });
});

route('GET', '/hub/skills/:name', (body, url, params) => {
  const rows = db.prepare('SELECT * FROM skills WHERE name = ? ORDER BY created_at DESC').all(params.get('name'));
  if (!rows.length) throw Object.assign(new Error('not found'), { status: 404 });
  return { name: rows[0].name, latest: { ...rows[0], files: JSON.parse(rows[0].files), tags: JSON.parse(rows[0].tags) },
    history: rows.map((r) => r.version) };
});

const server = createServer(async (req, res) => {
  const url = new URL(req.url, `http://127.0.0.1:${cfg.port}`);
  const send = (code, obj) => {
    const payload = JSON.stringify(obj);
    res.writeHead(code, { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(payload) });
    res.end(payload);
  };
  try {
    if (url.pathname === '/agentmemory/livez' && req.method === 'GET') {
      const h = routes.find((r) => r.method === 'GET' && r.path === url.pathname).handler;
      return send(200, h({}, url));
    }
    if (req.headers.authorization !== `Bearer ${SECRET}`) return send(401, { error: 'unauthorized' });

    for (const r of routes) {
      if (r.method !== req.method) continue;
      if (r.path === url.pathname) {
        const body = req.method === 'POST' ? await readBody(req) : {};
        const out = await r.handler(body, url, new Map());
        return send(200, out);
      }
      if (r.path.includes(':')) {
        const [p0] = r.path.split('/:');
        if (url.pathname.startsWith(p0 + '/')) {
          const params = new Map([['name', decodeURIComponent(url.pathname.slice(p0.length + 1))]]);
          const out = await r.handler({}, url, params);
          return send(200, out);
        }
      }
    }
    return send(404, { error: 'not found', path: url.pathname });
  } catch (e) {
    return send(e.status || 500, { error: String(e.message || e) });
  }
});

server.listen(cfg.port, '127.0.0.1', () => {
  console.log(`[hub] listening on 127.0.0.1:${cfg.port}  db=${DB_PATH}  upstream=${cfg.upstream}`);
  try { console.log('[hub] skills reindex', JSON.stringify(reindexSkills())); } catch (e) { console.error('[hub] reindex failed', e.message); }
  syncNow('boot').then((r) => console.log('[hub] boot sync', JSON.stringify(r)));
  setInterval(() => { syncNow('timer').catch(() => {}); }, cfg.syncIntervalMs || 60000);
  // Local snapshot is the offline backup: keep it fresh even when the NAS is down.
  setInterval(() => { try { writeSnapshot(); } catch (e) { console.error('[hub] snapshot failed', e.message); } }, 300000);
});
