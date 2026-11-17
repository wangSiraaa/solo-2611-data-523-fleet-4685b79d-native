# 531 — 费用账户与真实联查执行器

在 `materials/531` 下实现了「按来源实际读过的字节计费」的联查执行器。扫描费用由来源票据
（ticket）决定，而不是客户端自报、结果行数或投影后编码大小。

运行：

```bash
python3.11 -m unittest tests_531 -v     # 35 个验收场景
python3.11 probe.py                      # 演示输出（非验收）
python3.11 cli531.py run q1 --minimum 50 --then account,verify
```

## 文件

| 文件 | 职责 |
| --- | --- |
| `paged_source.py` | 两个真实 JSONL 分页来源：公开 `catalog`（snapshot、页数、每页 `upper_bound`），`scan(query_id,page_id)` 真实读文件页并返回稳定 `ticket_id`/扫描字节/行，`get_ticket` 返回 `done`/`not_started`/`unknown`，`verify_receipt` 作为票据事实来源。`upper_bound` 由 `splitlines(keepends=True)` 构造，含原始换行字节。 |
| `budget.py` | 独立的 `BudgetLedger`：`open_account`/`reserve`/`reserve_many`/`begin_scan`/`settle`/`release`/`state`/`audit`。余额 = 可用 + 有效预留 + 已花，恒等式与「全部非负」在每个变更后校验。 |
| `federation.py` | 真实发起两源页读，执行固定 `order.customer_id = customer.id` 的 inner join 及 `amount >= minimum` 过滤，保留重复订单；完整预留/逐页预留两种策略；明确终态 `completed`/`incomplete`/`cancelled`/`source_error`/`pending_cost`。 |
| `cli531.py` | `run`/`cancel`/`reconcile`/`account`/`query-status`/`verify` 六个动词。 |
| `tests_531.py` | 验收测试；断言只依据来源独立审计与真实账户表。 |
| `source.py` | 初始材料遗留的扁平文件来源，已被 `paged_source.py` 取代，不再被任何代码引用。 |

## 计费与账本语义

- 每实际扫描 1 字节收 1 credit，**包含随后被过滤掉的行**。`orders.jsonl` 在 `minimum=50`
  下只输出 1 行，但实际计费等于两个源审计的原始字节和（示例 155）。
- `settle` 只接受来源票据：金额取自 `scanned_bytes`，不取调用方参数。`Federation` 还为每个
  来源向账本注册 `verify_receipt`，账本据此把票据规范化到来源自己的回执，客户端无法伪造
  「预期已读」。
- 预留绑定账户、查询、来源与页身份，并记录 `ticket_id`。结算凭票据把预留中的实际费用转入
  已花，余量退回可用；`upper_bound` 大于实际时产生退款。
- **同票据重投**：同一 `ticket_id`、同一实际字节重复结算返回原结果，不二次扣费；同一来源
  票据落到第二个预留时全额退款（因为没有发生新的真实读）。换金额或冒用另一账户被拒绝
  （`TicketMismatch`/`WrongAccount`）。
- **未开始才可退**：`release` 只接受来源明确 `not_started` 的回执。查询取消、空结果、连接
  错误都不是免费证据。
- **unknown 保留足额义务**：扫描已发生但回执丢失时，页保持 `unknown`，预留全额继续占用，
  直到 `reconcile` 用来源票据补齐；绝不先退款再追账。
- **上界违约隔离**：若来源实际字节超过声明 `upper_bound`，预留进入 `quarantined`，报告
  `SourceBoundViolation`（含未覆盖金额），不从可用余额偷补、不伪造平账；`verify` 以
  `source_bound_violation` 显式报告。

## 两种预留策略与并发资金

- **整查询预留（`strategy="whole"`）**：一次原子预留全部页的上界。执行前资金全部锁定，
  并发查询在此期间可用余额最小，但一旦开始就不会中途因资金不足而停。预算不足时原子失败，
  整单释放、零扫描。
- **逐页预留（默认 `strategy="per_page"`）**：读每页前才预留该页上界。并发查询可交替获得
  资金，但可能读到一半因余额不足转 `incomplete`（只返回已完成部分诊断，不发布完整数据）。
  已扫页照常结算，未开始页在来源确认 `not_started` 后释放。

两条真实连接争最后额度时，`reserve` 的检查与扣减在同一把锁内完成，只有一个确实可支付的
一方拿到预留。测试 `test_concurrent_race_for_last_credits` 用两个线程和屏障验证：恰好一次
真实扫描，另一方 `incomplete`。

**未知费用为何不会被新查询占走**：`unknown` 页的预留仍处于有效预留（计入 `reserved`），
不回到 `available`。新查询只能使用 `available`，因此无法动用这笔仍欠来源的钱。`verify`
还会核对来源审计中尚未结算的真实读字节数不超过仍在保留的义务（`underheld_unknown`）。

## 执行与终态

- 页读**只在有效预留建立后**开始（`begin_scan` 把预留从 `active` 移到 `scanning`，关闭
  「取消与退款竞速」窗口）。
- 正常情况下先预留再扫描，再按票据结算。
- **预算不足**：不读未付费的页；返回 `incomplete` 及已完成部分诊断，`result=null`。
- **取消**：不再发新页；在途页收束为已结算，或保持 `unknown` 保留义务；只对来源确认
  `not_started` 的预留退款。已扫页不因取消/空结果/连接错误退费。
- **来源错误**：以来源票据分类（`done` 则结算，`not_started` 则退款，`unknown` 则保留），
  不因异常假定免费。
- **费用待决**：`pending_cost` 是终态，直到 `reconcile`。
- **reconcile** 只补齐原票据义务，绝不重跑整条联查；若所有页均已结算且未取消，才补出 join。

## 验收与审计

`verify` 不信任客户端计数：

1. 遍历两个来源的独立审计，得到真实扫描数与字节。
2. 每个审计票据都必须对应一个查询预留；没有预留的真实读 = `unpaid_read`。
3. 票据要么有账本结算且字节相等，要么处于合法 `unknown`（义务仍保留），否则报错。
4. 账本已花等于来源审计的已结算字节，账户恒等式成立、无负数。

测试覆盖：两查询争同余额、零命中仍收费、扫描后丢回执、迟到回执与取消、同票据重投、
上界大于实际的退款、源未开始时取消、并发抢最后额度、以及「额度改变后真实 scan 次数随之
改变」（`1` credits → 0 次，`10000` → 4 次）。
