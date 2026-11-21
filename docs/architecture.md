# 架构

> 本文描述自建 hub 归档前（2026-10-28 至 11-20）的实现行为；该 hub 现已迁回官方服务，代码保留作参考实现。
> 复用性（零依赖、配置可注入、无上游可用、stub 参考实现）见 README §4。

## 1. 组件

| 组件 | 位置 | 职责 | 失败时的表现 |
|---|---|---|---|
| hub 服务 | `src/hub.mjs` | 鉴权、路由、SQLite 读写、outbox 重放、快照、技能索引 | 进程退出；重启后从本地库恢复，outbox 继续重试 |
| SQLite 库 | `hub.db`（WAL） | `memories` / `outbox` / `skills` / `meta` | 单文件损坏可从快照 + 备份恢复 |
| 快照 | `snap/latest.json` | 全量导出的原子副本；离线时也每 5 min 刷新 | 缺失不影响服务，只影响灾备 |
| 备份 | `backups/hub-<ts>.json` | 手工归档，保留最近 14 份 | 同上 |
| 隧道 | 3112 → 上游 | 把远端映射到 loopback；keepalive 自动重连 | hub 转 offline，写继续成功 |
| 看门狗 | `tools/tunnel-watchdog.py` | 端口掉了才拉起，单实例锁 | 不启动也不影响已运行的 hub |
| 桥 | `src/mem.mjs` | 显式调用：`put` / `get` / `list` / `status` | 报错退出，不注入上下文 |

## 2. 数据模型

```sql
memories(id TEXT PK, upstream_id TEXT, title, content, type, tags JSON,
         project, files JSON, scope, origin, created_at, updated_at,
         version INT, remote INT, content_id TEXT, entity_id TEXT)
outbox(seq INTEGER PK AUTOINCREMENT, kind, payload JSON, created_at,
       synced_at, upstream_id, error)          -- kind: remember | tombstone
tombstones(cid TEXT PK, entity_id, deleted_at, origin)
skills(name, version, description, content, files JSON, tags JSON, created_at,
       PRIMARY KEY(name, version))     -- 由磁盘目录重建的只读索引
meta(key TEXT PK, value)                -- last_sync_ok / last_sync_report / last_reconcile
```

`upstream_id` 是幂等键：本地行记住“我在上游叫什么”，拉取时据此跳过自己。

## 3. 四条主链路

### 3.1 写（`POST /agentmemory/remember`）

```
校验 scope → redact(title/content) → INSERT memories
          → scope != private ? INSERT outbox : (跳过)
          → 若 upstream 在线，异步触发一次 sync
          → 200 {success, redacted, memory}
```

顺序不能反：先落库再排队，任何一步失败都不会让调用方以为“写成功了但什么都没有”。

### 3.2 读（`POST /agentmemory/smart-search`）

只查本地库。打分 = 命中权重（标题 3 / 标签 2 / 正文 1）归一后 0.85 + 30 天半衰 recency 0.15。技能与记忆在同一个响应里排序，命中项用 `layer` 区分。**不查上游**：远端不可用时检索仍然完整。

### 3.3 同步（定时 60 s / 手动 / 写入后异步）

```
single-flight 保护
  → GET  livez（探活，失败即 offline 并结束）
  → GET  /agentmemory/export        # 上行
       按 id upsert；若 id 命中本地 upstream_id 则跳过   ← 去重
  → 遍历 outbox WHERE synced_at IS NULL
       POST /agentmemory/remember
       成功：回填 synced_at / upstream_id / 清 error
       失败：写 error，保留待下轮
  → 写快照（tmp + rename）
```

单飞（single-flight）是必须的：定时器、写入触发、手动同步三者会重叠，重叠的重放会把同一条记录推两次。

### 3.4 删除（`POST /agentmemory/forget`）

```
记墓碑（cid → deleted_at）
  → 删 memories 行
  → 清掉 outbox 中该 id 的待推送（否则恢复后复活）
  → 尽力转发上游 forget（失败不回滚）
  → 入队一条墓碑记录（remember，type=tombstone，tags=tomb:<cid>）
```

### 3.5 修订（`POST /agentmemory/revise`）

```
读旧行 → 脱敏新内容 → 新 cid → 原地更新（updated_at 刷新、version+1）
      → 入队一条同 eid 的新记录（其它机器按 eid LWW 覆盖）
```

### 3.6 对账（`POST /hub/reconcile`）

```
pull（对齐到同一快照）
  → 按 cid 分组：重复组保留最新，另发上游 forget
  → 按 eid 分组：旧版本发上游 forget
  → 本地按 cid 去重
  → 本地已推但上游缺失的行重新入队（self-heal）
  → 过期墓碑：删本地标记 + forget 上游墓碑记录
```

## 4. 不变量

1. 本地写成功即返回成功，上游可达性只影响同步报告。
2. 同一条记忆在本地最多一行；`upstream_id` 唯一解释“这条是否已上浮”。
3. 同一内容在本地最多一行；`content_id`（cid）唯一解释“这是不是同一条事实”。
4. `private` 不产生 outbox 行，因此不存在“忘了过滤”的路径。
5. 落库内容一定已经过脱敏；库里的正文不含匹配到的凭据。
6. 技能只有磁盘一个写者；HTTP 写入恒为 409。
7. 快照要么是旧的完整文件，要么是新的完整文件（tmp + rename），不会是半个。
8. 单写者：同一时刻只有一个进程打开主库。
9. 墓碑不进 `memories`、不进检索、不进快照；它只在同步与对账时被读取。
10. 同一 cid 在墓碑 TTL 内不允许重新落库（409），TTL 过后允许。

## 5. 故障对照

| 现象 | 根因 | 处理 |
|---|---|---|
| 远端出现重复记录 | 拉取未按 `upstream_id` 去重 | 修复后以 `added:0` 回归 |
| 条数对不上、尾部丢失 | WAL 被第二进程 checkpoint | 单写者 + 停服后再 checkpoint |
| `fetch failed` 但本机服务正常 | 代理变量被 fetch 应用到 loopback | 桥与看门狗在子进程里清空 proxy 变量 |
| 中文变 `U+FFFD` | 非 ASCII 走 argv 被转本地代码页 | 一律走 UTF-8 文件体（`put-json --file`） |
| 删除几分钟后复活 | outbox 里仍有该条待推送 | forget 同时清 outbox（已实现并有断言） |
| 隧道静默消失 | 对端 reset，ssh 退出 | 隧道常驻脚本带 `ServerAliveInterval` 重连 |
