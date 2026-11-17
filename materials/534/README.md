# 534 — 来源维护排空与等价备用源批次采集

批次按**冻结 descriptor** 派发：每个逻辑 shard 用真实文件读取、按字节数 +
SHA-256 + CSV 行范围校验后导入 SQLite。来源 A 进入维护时按**真实在途
ticket** 排空；尚未开始分片才由有资格的等价备用源 B 接替。

## 目录

| 文件 | 作用 |
|---|---|
| `descriptor.py` | 冻结 descriptor 的构建、校验与规范化 SHA256；CSV 解析 |
| `controlled_source.py` | 可控真实源 fixture：`before_read`/`before_callback` 暂停门、`close` 拒绝活跃读 |
| `harness.py` | 测试/演示用真实 A/B 文件树与 `CompositeSource`（按 shard 分文件） |
| `source_registry.py` | 来源状态机、ticket 仲裁、维护请求、带代次的真实 close |
| `ingestion.py` | 按 descriptor 派发读取、descriptor 校验、幂等回调、SQLite 导入、verify |
| `service.py` | 组合两者并把顺序状态写入 `state.json` |
| `cli.py` | 命令：`init start-batch step maintain cancel-maintenance cancel-batch source-status batch-status verify` |
| `tests.py` | 20 个验收测试（含竞争、幂等、摘要不符、零行片） |
| `probe.py` | 轻量复现：维护排空、真实 B 读取、摘要不符保持 UNAVAILABLE |
| `demo.sh` | 跨进程 CLI 演练 |

运行：

```bash
cd materials/534
python3.11 tests.py
python3.11 probe.py
./demo.sh
```

## descriptor（不变量）

```json
{
  "format": "batch-descriptor/1",
  "batch_id": "...",
  "snapshot": "...",
  "primary": "A",
  "backups": ["B"],
  "shards": [
    {"shard_id": "shard-1", "bytes": 504, "sha256": "<64 hex>",
     "snapshot": "...", "first_row": 1, "last_row": 50, "row_count": 50,
     "columns": ["row_id","value"], "origin": "sources/A/shard-1.csv"}
  ]
}
```

descriptor 只描述、不被改写。`verify` 用 `origin` 独立重新打开原始 CSV，逐行
核对 `(shard_id, row_no)` 与整数总量；`incomplete` 只列出未完成片，零行合法片
是 `COMPLETE` 且总量为 0，绝不与“未取得”混淆。

来源资格 = **可读（非 CLOSED/FAILED、无生效维护）且 snapshot 等于该片
snapshot**；读取后仍按 descriptor 的字节数 + SHA-256 + 行数校验。文件名相同
从不构成资格。

## SourceRegistry 协议

状态：`OPEN → DRAINING → CLOSED`，异常为 `FAILED`。维护请求 `request_id`
可幂等重放（返回进度、不重开）。`acquire(source,batch,shard)` 与维护请求的
先后语义：

1. 维护生效**之后**拒绝该源的新 ticket（`MAINTENANCE_ACTIVE` / `SOURCE_CLOSED`）。
2. 生效**之前**已授予的 ticket（无论是否已收到字节）都是合法在途，被采纳进
   新代次并允许完成；不会因为“尚未收到数据”被无条件重派。
3. 只有**尚未取得 ticket** 的分片，在维护生效后由下一轮派发改读有资格的 B。

`release(ticket)` 是“分片完成 → 允许关闭”的唯一汇合点：若源在 DRAINING 且
已无在途 ticket，release 直接驱动 **真实 close**。`close` 只有 fixture 真正
关闭成功且代次未变才把状态置 `CLOSED`——不是给字段赋值。close 抛
`ACTIVE_READS` 时维护保持 `ACTIVE`、`error`/在途清单保留、不会吞成 `CLOSED`。

### 代次（generation）保护

`acquire`、维护请求、取消、真实 close 都带单调 `generation`：

- 取消只在底层尚未关闭时允许，成功后 `generation += 1` 并回到 `OPEN`；
  同一时刻只剩代次匹配的 close 回调可关闭。
- 锁顺序固定为 `source.close_lock → registry.lock`。取消要么先赢锁并令挂起
  的 close 回调变为 `STALE_CLOSE`，要么等 close 完成并因真实已关而拒绝
  `ALREADY_CLOSED`。旧回调永远不能关闭刚恢复使用的来源。

## Ingestion 派发与幂等

每个片身份是 `(batch_id, shard_id)`：即使两个片 CSV 字节完全相同，也分别
处理。行身份是 `(shard_id, row_no)`（主键 `rows(batch_id,shard_id,row_no)`）。

流程：`acquire → mark_reading → 真实 read → descriptor 校验 → 暂存 → callback 提交 → release`。

- **读取前**：`open → reading` 在 registry 锁内原子完成，所以维护不可能在
  这之后把合法在途工作判成“未开始”。
- **读取后、回调前**：片处于 `PENDING_DELIVERY`，ticket 仍被持有；此时取消
  batch 会丢弃暂存、释放 ticket，且绝不会第二次导入。回调带 `ticket/attempt`
  （`token`）；迟到失败、重复完成、取消交错都只提交至多一次。
- 同一分片两个派发者用每片非阻塞锁：第二个得到 `IN_FLIGHT`，不会启动第二次
  真实读。
- 取消整个 batch：递增 `cancel_epoch`；已取但未开始读的 ticket 被 abort/释放，
  正在读的由 worker 在读取返回后收束并释放，最后对来源 `tick` 使维护可关闭。

## 设计取舍（集中仲裁 + 事务登记 / 带代次回调）

采用**集中仲裁 ticket** + SQLite 事务登记（`shards_done` 唯一键）+
**带代次回调**的组合：

- ticket/状态在 registry 锁内集中仲裁，使“维护决定 → 下一次真实来源选择”
  与“完成/释放 → 是否允许关闭”成为同一临界区的两个方向。
- 片提交用 `BEGIN IMMEDIATE` + `(batch_id,shard_id)` 唯一键，保证即使回调
  重放也只有一次可见提交；这比依赖回调“恰好一次”更稳。
- 代次让“维护撤销/关闭时点”与“片完成提交”可以安全交错：关闭只属于当前
  代次；旧代次的 close 回调（取消后残留）被拒为 `STALE_CLOSE`，不会误关
  刚恢复的来源。取舍是：**取消优先于进行中的关闭**（谁先拿到
  `close_lock` 谁定序），一旦真实 close 完成则不可复活。

## 正确性范围

2 源、1 批、1–8 片、每片 50 行、两个派发者、一个维护调用者；来源只读、
descriptor 不变。不做动态发现、任务范围分裂、凭据轮换、跨机租约或崩溃恢复
（顺序 CLI 状态不含在途 ticket）。
