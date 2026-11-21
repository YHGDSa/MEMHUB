#!/usr/bin/env node
// Explicit-call bridge to the local memory hub (127.0.0.1:3111).
//
// Nothing is injected automatically: an agent only learns about another
// agent's memory when it is told to run this command. That is the whole
// point -- recall is an action, not an ambient side effect.
//
//   node mem.mjs put --title T --content C [--content-file F] [--tags a,b]
//                    [--scope global|project|agent|private] [--project P]
//                    [--agent codex]
//   node mem.mjs get --query Q [--limit 5]
//   node mem.mjs list [--limit 20]
//   node mem.mjs status
//
// The bearer secret is read from the config's secretFile by this script, so it
// never has to appear in a prompt, a command line, or a log.
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
// One config for both hub and CLI: pass HUB_CONFIG or put config.json at the
// repo root, so the secret path never has to be repeated per call.
const CONFIG_PATH = process.env.HUB_CONFIG || join(HERE, '..', 'config.json');
const cfg = JSON.parse(readFileSync(CONFIG_PATH, 'utf8'));
const base = process.env.MEM_BASE || `http://127.0.0.1:${cfg.port}`;
const secret = readFileSync(resolve(dirname(CONFIG_PATH), cfg.secretFile), 'utf8').trim();

// Node 22's built-in fetch honours HTTP_PROXY even for 127.0.0.1 and ignores
// NO_PROXY. The local hub / official tunnel is always loopback, so drop proxy
// vars for this short-lived process to avoid false "fetch failed" errors.
for (const k of ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']) {
  delete process.env[k];
}

function arg(name, fallback = undefined) {
  const i = process.argv.indexOf(`--${name}`);
  return i > 0 && process.argv[i + 1] !== undefined ? process.argv[i + 1] : fallback;
}

async function call(method, path, body) {
  const res = await fetch(base + path, {
    method,
    headers: {
      Authorization: `Bearer ${secret}`,
      'Content-Type': 'application/json; charset=utf-8',
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await res.text();
  let json;
  try {
    json = JSON.parse(text);
  } catch {
    json = { raw: text };
  }
  if (!res.ok) {
    const err = new Error(`${res.status} ${json.error || text.slice(0, 200)}`);
    err.body = json;
    throw err;
  }
  return json;
}

const cmd = process.argv[2];

try {
  let out;
  if (cmd === 'put') {
    const content = arg('content-file')
      ? readFileSync(resolve(arg('content-file')), 'utf8')
      : arg('content');
    if (!content) throw new Error('--content or --content-file required');
    const tags = (arg('tags', '') || '').split(',').map((t) => t.trim()).filter(Boolean);
    const agent = arg('agent');
    if (agent) tags.push(`agent:${agent}`);
    out = await call('POST', '/agentmemory/remember', {
      title: arg('title', content.slice(0, 60)),
      content,
      tags,
      scope: arg('scope', 'global'),
      project: arg('project', null),
      type: arg('type', 'fact'),
    });
  } else if (cmd === 'put-json') {
    // Preferred for non-ASCII content from foreign shells: everything comes
    // from one UTF-8 file, so no argv re-encoding can corrupt it.
    const file = arg('file');
    if (!file) throw new Error('--file required');
    const m = JSON.parse(readFileSync(resolve(file), 'utf8'));
    if (!m.content) throw new Error('file must contain content');
    const tags = Array.isArray(m.tags) ? m.tags : [];
    if (m.agent) tags.push(`agent:${m.agent}`);
    out = await call('POST', '/agentmemory/remember', {
      title: m.title || String(m.content).slice(0, 60),
      content: m.content,
      tags,
      scope: m.scope || 'global',
      project: m.project || null,
      type: m.type || 'fact',
    });
  } else if (cmd === 'get') {
    const query = arg('query');
    if (!query) throw new Error('--query required');
    out = await call('POST', '/agentmemory/smart-search', {
      query,
      limit: Number(arg('limit', 5)),
    });
  } else if (cmd === 'list') {
    out = await call('GET', `/agentmemory/memories?limit=${Number(arg('limit', 20))}`);
  } else if (cmd === 'status') {
    out = await call('GET', '/hub/status');
  } else {
    console.error('usage: mem.mjs put|get|list|status [--key value ...]');
    process.exit(2);
  }
  process.stdout.write(JSON.stringify(out, null, 2) + '\n');
} catch (e) {
  console.error(`mem: ${e.message}`);
  process.exit(1);
}
