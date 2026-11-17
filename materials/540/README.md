# Finite standby protocol — hot-standby conflict handling

`db.py` is a SQLite-backed row-version store; `Replay` (replication client) and
`Readers` (interactive report clients) each own an **independent connection**.
No server, network, leader election or general SQL is involved.

Quick start:

```
python3 probe.py          # the three failure cases from the task
python3 selftest.py       # protocol + oracle checks (all scenarios)
python3 plan.py plan.json # replay a JSON operation trace (see below)
```

## Public surface

Replication client:

```python
r = Replay(path, grace=5)
r.feed(record, now=0)     # -> {'status': 'APPLIED'|'WAITING', ...}
r.inspect()               # coherent view of the standby
r.shutdown()
```

Report client:

```python
q = Readers(path)
q.begin(query_id)         # pins visible_commit
q.next(query_id, n)       # -> up to n rows, UTF-8 byte order; [] when exhausted
q.status(query_id)        # -> {id, snapshot, last_key, state, cancel_lsn, ...}
q.close(query_id)         # idempotent
q.next(...)               # raises ConflictCancelled(query_id, cancel_lsn)
```

Both services are independently usable: the replication client never needs a
reader, the reader never needs the replication client, and each works on its
own connection. `shutdown()` closes it; a new instance on the same path
reopens the same state.

## Semantics

### Transactions and publication

A `put` is a plain assignment into the transaction's **private view**; repeated
puts on one key follow source order. A `delete` must name a key present in that
private view. `commit` publishes **all touched keys atomically, tagged with the
commit record's LSN**; until then nothing is visible to any report.
`abort` discards the private view. `applied_lsn` (processed contiguous prefix)
and `visible_commit` (last published commit) are tracked separately.

The private view is stored durably as `tx_base` (a copy of the committed view at
`begin` time) plus `tx_touched` (the transaction's final state per key). This is
what lets a transaction restart mid-flight after a reopen and still resolve
`delete` against exactly what the transaction could see.

### Idempotent, ordered input

Each accepted LSN is recorded in `records` with a canonical fingerprint and its
original result. Redelivery of identical content returns the original result
(`duplicate: true`); different content at an already accepted LSN raises
`CONFLICTING_REDELIVERY`. A rejected or unfinished record never advances
`applied_lsn`: a gap raises `LSN_GAP`, and a later LSN while a cleanup waits
raises `CLEANUP_PENDING`.

### Cleanup, grace and cursors

`cleanup(horizon)` is a logical revocation of old snapshots. A live query with
`snapshot < horizon` is a conflict; `snapshot == horizon` is not. On **first**
receipt the deadline is fixed at `now + grace` (`now` is caller-supplied
non-negative logical milliseconds, monotonically non-decreasing across calls,
`grace` fixed at construction).

* Conflicting queries still active before the deadline → `WAITING`;
  `applied_lsn` stays in front of the cleanup, so nothing overtakes it.
* Queries close → the same cleanup re-sent applies immediately.
* At the deadline, still-active conflicts are moved to `CANCELLED` (with the
  triggering cleanup LSN), then versions with `dead <= horizon` are deleted —
  both in **one transaction**, so no state ever sees "cleaned but a cursor still
  runs".
* New queries cannot extend a deadline: a query begun after the cleanup has
  `snapshot == visible_commit >= horizon` and is never a conflict. A query that
  was already running when the cleanup was first seen shares the original
  deadline, even across a reopen.

Cancelled queries hold their logical snapshot but cannot page: `next` raises
`ConflictCancelled` carrying `cancel_lsn`. `close` is idempotent and never turns
a `CANCELLED` terminal state into a successful `CLOSED`.

### Durability and reopen

Every `feed`/`next`/`begin`/`close` commits before returning
(`synchronous=FULL`). Pending deadlines, cursor positions, transaction private
views, query terminal states, the last horizon and the logical clock are all on
disk. A reopened process resumes exactly in the middle of a transaction, a
pending cleanup, or a half-read report. WAL + `BEGIN IMMEDIATE` give the two
independent connections a stable cross-connection view.

## Storage organisation and trade-offs

| state | where | why |
|---|---|---|
| private tx view | `tx_base` + `tx_touched` per open tx | O(keys the tx could touch) copy at begin; bounded by the 200-key universe, and keeps `delete` and source-order puts exact after a reopen. Alternative (store only puts and check the `versions` history) is smaller but needs more recovery logic. |
| accepted stream | `records` (fingerprint + stored result) | O(WAL) rows; enables exact idempotency and conflict rejection. Costs one row per accepted LSN, which is cheap at 300 records and keeps recovery trivially explainable. |
| waiting cleanup | single `pending` row | Constant space; the deadline is persisted once and never re-timed. Serialising on one pending cleanup keeps `applied_lsn` honestly contiguous instead of tracking a skip-set. |
| versions | `versions` with inclusive `born`/exclusive `dead` | Snapshot reads are an index range scan; cleanup is a single `dead <= horizon` delete. Old versions are retained until a cleanup revokes them, which is what lets an old cursor keep reading without silent row loss. |

The main trade-off is **write amplification for simplicity**: a commit rewrites
each touched key's live version, begins copy their whole visible view, and every
LSN is journalled. In exchange, reads are pure indexed scans, recovery is
"read the tables", and the conflict protocol is a single durable state machine
with no in-memory bookkeeping to reconstruct. For the stated limits (≤200 keys,
≤20 interleaved transactions, ≤10 queries, ≤300 WAL records, single-process
controlled calls) this is comfortably small.

## Replayable operation plans

`plan.py` runs a JSON plan interleaving `feed`, `begin`, `next`, `close`,
`status`, `inspect`, `continue_pending` and `reopen`, printing every real return
value (or error) plus an `inspect` after each step. Operations are listed in
[`plan.py`](plan.py); a summary of one cross-reopen run is in
[`PLAN_TRACE.md`](PLAN_TRACE.md).

A working database reopened at the end is saved at `work.sqlite`;
`python3 plan.py plan.json --db work.sqlite` continues it. `plan.py` checkpoints
and closes cleanly, so the file is self-contained after a run.

## Notes

`Replay.feed` is idempotent, so re-sending the cleanup record is how a caller
polls an already-waiting cleanup (there is also `continue_pending(now)`).
`ConflictCancelled` lives in `readers.py`; import it from there.
