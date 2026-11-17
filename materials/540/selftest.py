"""Independent reference model + tests for the standby protocol.

The oracle intentionally does not import the storage-layer read helpers.  It
replays the committed transaction stream into a plain dict per snapshot using
its own rule: a transaction's private view is "committed view at begin time +
that transaction's own source-order writes"; a commit with LSN c makes the
result visible for every snapshot S >= c.

Run: python3 selftest.py
"""
import json
import tempfile
from pathlib import Path

from readers import ConflictCancelled, Readers
from replay import Replay


# --------------------------------------------------------------------- oracle

def oracle_views(records):
    """Return {commit_lsn: {key: value}} after every commit in ``records``.

    Ground truth for what a query pinned at commit LSN c must see.  A
    transaction's own writes are buffered privately and applied, in commit
    order, onto the shared state at the moment it commits (covering only the
    keys it touched), exactly like the replica's atomic publish.
    """
    state = {}
    touched = {}
    views = {}
    for rec in sorted(records, key=lambda r: r['lsn']):
        kind, txid = rec.get('kind'), rec.get('tx')
        if kind == 'begin':
            touched[txid] = {}
        elif kind == 'put':
            touched[txid][rec['key']] = ('put', rec['value'])
        elif kind == 'delete':
            touched[txid][rec['key']] = ('del', None)
        elif kind == 'commit':
            for key, (op, value) in touched.pop(txid).items():
                if op == 'put':
                    state[key] = value
                else:
                    state.pop(key, None)
            views[rec['lsn']] = dict(state)
        elif kind == 'abort':
            touched.pop(txid)
    return views


def oracle_page(view, after_key, n):
    keys = sorted((k for k in view), key=lambda k: k.encode('utf-8'))
    if after_key is not None:
        keys = [k for k in keys if k.encode('utf-8') > after_key.encode('utf-8')]
    return [{'key': k, 'value': view[k]} for k in keys[:n]]


class Check:
    def __init__(self):
        self.failures = []

    def eq(self, name, got, want):
        if got != want:
            self.failures.append('%s: got %r want %r' % (name, got, want))

    def ok(self, name, cond, detail=''):
        if not cond:
            self.failures.append('%s: %s' % (name, detail))


# ------------------------------------------------------------------ scenarios

def base_records():
    return [
        {'lsn': 1, 'kind': 'begin', 'tx': 'seed'},
        {'lsn': 2, 'kind': 'put', 'tx': 'seed', 'key': 'a', 'value': 10},
        {'lsn': 3, 'kind': 'put', 'tx': 'seed', 'key': 'b', 'value': 20},
        {'lsn': 4, 'kind': 'commit', 'tx': 'seed'},
    ]


def run_scenario(name, body):
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / 'replica.sqlite'
        return body(str(p))


def bump_b(r, start_lsn, value):
    """Feed a one-key transaction that rewrites b and returns the new LSN."""
    recs = [
        {'lsn': start_lsn, 'kind': 'begin', 'tx': 'u%d' % start_lsn},
        {'lsn': start_lsn + 1, 'kind': 'put', 'tx': 'u%d' % start_lsn,
         'key': 'b', 'value': value},
        {'lsn': start_lsn + 2, 'kind': 'commit', 'tx': 'u%d' % start_lsn},
    ]
    for rec in recs:
        r.feed(rec)
    return start_lsn + 2


def test_interleaved_and_snapshot(check):
    def body(path):
        r, q = Replay(path, grace=5), Readers(path)
        for rec in base_records():
            r.feed(rec)
        q.begin('old')
        first = q.next('old', 1)
        check.eq('snapshot.first', first, [{'key': 'a', 'value': 10}])

        interleaved = [
            {'lsn': 5, 'kind': 'begin', 'tx': 'A'},
            {'lsn': 6, 'kind': 'put', 'tx': 'A', 'key': 'x', 'value': 99},
            {'lsn': 7, 'kind': 'begin', 'tx': 'B'},
            {'lsn': 8, 'kind': 'put', 'tx': 'B', 'key': 'b', 'value': 30},
            {'lsn': 9, 'kind': 'commit', 'tx': 'B'},
        ]
        for rec in interleaved:
            r.feed(rec)
        # Uncommitted A must not leak into a query begun now.
        q.begin('leak')
        leak = q.next('leak', 20)
        check.eq('interleaved.no_leak', leak,
                 [{'key': 'a', 'value': 10}, {'key': 'b', 'value': 30}])
        # And the older cursor keeps its frozen view.
        rest = q.next('old', 10)
        check.eq('snapshot.rest', rest, [{'key': 'b', 'value': 20}])

        # Private view: A put then delete, then re-put before commit.
        r.feed({'lsn': 10, 'kind': 'put', 'tx': 'A', 'key': 'x', 'value': 1})
        r.feed({'lsn': 11, 'kind': 'put', 'tx': 'A', 'key': 'x', 'value': 2})
        r.feed({'lsn': 12, 'kind': 'delete', 'tx': 'A', 'key': 'x'})
        r.feed({'lsn': 13, 'kind': 'abort', 'tx': 'A'})
        # A never published anything; abort of an uncommitted tx is invisible.
        q.begin('after_abort')
        check.eq('abort.invisible', q.next('after_abort', 20), leak)
        r.shutdown(); q.shutdown()
    run_scenario('interleaved', body)


def test_paging_against_oracle(check):
    recs = base_records()
    recs += [
        {'lsn': 5, 'kind': 'begin', 'tx': 't2'},
        {'lsn': 6, 'kind': 'put', 'tx': 't2', 'key': 'ä', 'value': 7},
        {'lsn': 7, 'kind': 'put', 'tx': 't2', 'key': 'A', 'value': 8},
        {'lsn': 8, 'kind': 'put', 'tx': 't2', 'key': 'a', 'value': 11},
        {'lsn': 9, 'kind': 'put', 'tx': 't2', 'key': 'z', 'value': 9},
        {'lsn': 10, 'kind': 'commit', 'tx': 't2'},
        # delete a then reinsert in a later tx.
        {'lsn': 11, 'kind': 'begin', 'tx': 't3'},
        {'lsn': 12, 'kind': 'delete', 'tx': 't3', 'key': 'a'},
        {'lsn': 13, 'kind': 'commit', 'tx': 't3'},
        {'lsn': 14, 'kind': 'begin', 'tx': 't4'},
        {'lsn': 15, 'kind': 'put', 'tx': 't4', 'key': 'a', 'value': 42},
        {'lsn': 16, 'kind': 'commit', 'tx': 't4'},
    ]
    views = oracle_views(recs)

    def body(path):
        r, q = Replay(path, grace=999), Readers(path)
        for rec in recs:
            r.feed(rec)
        check.eq('oracle.has_two_commits', sorted(views), [4, 10, 13, 16])
        for qid, snap in (('s4', 4), ('s10', 10), ('s13', 13), ('s16', 16)):
            # Pin the snapshot by replaying just enough to reach it.
            pass
        # Recreate each snapshot via a fresh DB prefix.
        for snap in (4, 10, 13, 16):
            with tempfile.TemporaryDirectory() as tmp:
                pp = str(Path(tmp) / 'p.sqlite')
                rr, qq = Replay(pp, grace=999), Readers(pp)
                for rec in recs:
                    if rec['lsn'] <= snap:
                        rr.feed(rec)
                qq.begin('cur')
                got = []
                while True:
                    pg = qq.next('cur', 2)
                    if not pg:
                        break
                    got.extend(pg)
                check.eq('paging.snapshot_%d' % snap, got,
                         oracle_page(views[snap], None, 10 ** 6))
                # Empty final page and fixed view hold until close.
                check.eq('paging.exhausted_%d' % snap, qq.next('cur', 2), [])
                qq.close('cur'); qq.close('cur')  # idempotent
                check.eq('paging.close_state_%d' % snap,
                         qq.status('cur')['state'], 'CLOSED')
                rr.shutdown(); qq.shutdown()
        r.shutdown(); q.shutdown()
    run_scenario('paging', body)


def test_cleanup_variants(check):
    # (a) no conflict -> APPLIED immediately, old versions gone.
    def no_conflict(path):
        r = Replay(path, grace=5)
        for rec in base_records():
            r.feed(rec)
        # rewrite b at lsn 5..7 so born=4 b dies at 7.
        for rec in [
            {'lsn': 5, 'kind': 'begin', 'tx': 'u'},
            {'lsn': 6, 'kind': 'put', 'tx': 'u', 'key': 'b', 'value': 50},
            {'lsn': 7, 'kind': 'commit', 'tx': 'u'},
        ]:
            r.feed(rec)
        res = r.feed({'lsn': 8, 'kind': 'cleanup', 'horizon': 7}, now=0)
        check.eq('cleanup.none.status', res['status'], 'APPLIED')
        ins = r.inspect()
        check.eq('cleanup.none.applied', ins['applied_lsn'], 8)
        check.eq('cleanup.none.versions',
                 [(v['key'], v['born']) for v in ins['versions']],
                 [('a', 4), ('b', 7)])
        r.shutdown()
    run_scenario('no_conflict', no_conflict)

    # (b) query closes in time -> next feed same cleanup applies immediately,
    #     and progress then continues.  Horizon must exceed the pinned
    #     snapshot to count as a conflict, so commit b again first.
    def closes_in_time(path):
        r, q = Replay(path, grace=50), Readers(path)
        for rec in base_records():
            r.feed(rec)
        q.begin('early')                 # snapshot 4
        commit = bump_b(r, 5, 50)        # commit at LSN 7
        res = r.feed({'lsn': 8, 'kind': 'cleanup', 'horizon': commit}, now=0)
        check.eq('cleanup.close.waiting', res['status'], 'WAITING')
        check.eq('cleanup.close.applied_stuck', r.inspect()['applied_lsn'],
                 commit)
        q.close('early')
        res = r.feed({'lsn': 8, 'kind': 'cleanup', 'horizon': commit}, now=10)
        check.eq('cleanup.close.applied', res['status'], 'APPLIED')
        res = r.feed({'lsn': 9, 'kind': 'begin', 'tx': 'n'})
        check.eq('cleanup.close.next_lsn_ok', res['status'], 'APPLIED')
        r.shutdown(); q.shutdown()
    run_scenario('closes_in_time', closes_in_time)

    # (c) shared grace expires -> conflicting cursors CANCELLED, applied, new
    #     queries still work.  A newly begun query has snapshot ==
    #     visible_commit >= horizon, so it never conflicts; it must neither
    #     extend the deadline nor be cancelled.
    def grace_expiry(path):
        r, q = Replay(path, grace=10), Readers(path)
        for rec in base_records():
            r.feed(rec)
        q.begin('c1')                    # snapshot 4
        q.begin('c2')                    # snapshot 4
        commit = bump_b(r, 5, 50)        # commit at LSN 7, snapshot still old
        res = r.feed({'lsn': 8, 'kind': 'cleanup', 'horizon': commit}, now=100)
        check.eq('cleanup.grace.waiting', res['status'], 'WAITING')
        check.eq('cleanup.grace.deadline', res['deadline'], 110)
        # Re-sending the same cleanup before the deadline must not re-time it
        # and must stay WAITING, not advance.
        res = r.feed({'lsn': 8, 'kind': 'cleanup', 'horizon': commit}, now=105)
        check.eq('cleanup.grace.retimed', res['deadline'], 110)
        check.eq('cleanup.grace.still_waiting', res['status'], 'WAITING')
        # A later LSN cannot overtake the pending cleanup.
        try:
            r.feed({'lsn': 10, 'kind': 'begin', 'tx': 'x'})
            check.ok('cleanup.grace.blocked', False, 'later LSN slipped in')
        except ValueError as exc:
            check.eq('cleanup.grace.blocked', str(exc), 'CLEANUP_PENDING')
        # A query begun while waiting does not extend the deadline.
        q.begin('late')                  # snapshot 7 == horizon, not a conflict
        check.eq('cleanup.grace.late_snapshot',
                 q.status('late')['snapshot'], commit)
        res = r.feed({'lsn': 8, 'kind': 'cleanup', 'horizon': commit}, now=110)
        check.eq('cleanup.grace.applied', res['status'], 'APPLIED')
        check.eq('cleanup.grace.deadline_kept', res['deadline'], 110)
        check.eq('cleanup.grace.cancelled', sorted(res['cancelled']),
                 ['c1', 'c2'])
        check.eq('cleanup.grace.applied_lsn', r.inspect()['applied_lsn'], 8)
        for qid in ('c1', 'c2'):
            try:
                q.next(qid, 1)
                check.ok('cleanup.grace.raises', False, qid)
            except ConflictCancelled as exc:
                check.eq('cleanup.grace.cancel_lsn.%s' % qid, exc.cancel_lsn, 8)
        # The non-conflicting late query survives and can read.
        check.eq('cleanup.grace.late_state', q.status('late')['state'],
                 'RUNNING')
        check.eq('cleanup.grace.late_reads', q.next('late', 10),
                 [{'key': 'a', 'value': 10}, {'key': 'b', 'value': 50}])
        # Cancelled cannot be closed into success.
        st = q.close('c1')
        check.eq('cleanup.grace.close_keeps_cancelled', st['state'], 'CANCELLED')
        check.eq('cleanup.grace.close_idempotent', q.close('c1')['state'],
                 'CANCELLED')
        # New queries still work against the now-clean store.
        q.begin('fresh')
        check.eq('cleanup.grace.fresh', q.next('fresh', 10),
                 [{'key': 'a', 'value': 10}, {'key': 'b', 'value': 50}])
        r.shutdown(); q.shutdown()
    run_scenario('grace_expiry', grace_expiry)

    # snapshot == horizon does not conflict.
    def boundary(path):
        r, q = Replay(path, grace=5), Readers(path)
        for rec in base_records():
            r.feed(rec)
        q.begin('at_horizon')  # snapshot 4
        res = r.feed({'lsn': 5, 'kind': 'cleanup', 'horizon': 4}, now=0)
        check.eq('boundary.applied', res['status'], 'APPLIED')
        check.eq('boundary.not_cancelled', q.status('at_horizon')['state'],
                 'RUNNING')
        # The cursor at horizon keeps reading its (still complete) view.
        check.eq('boundary.reads', q.next('at_horizon', 10),
                 [{'key': 'a', 'value': 10}, {'key': 'b', 'value': 20}])
        r.shutdown(); q.shutdown()
    run_scenario('boundary', boundary)


def test_redelivery_and_reopen(check):
    def body(path):
        r = Replay(path, grace=5)
        for rec in base_records():
            r.feed(rec)
        # Same content redelivered -> same outcome, not published again.
        again = r.feed({'lsn': 2, 'kind': 'put', 'tx': 'seed', 'key': 'a',
                        'value': 10})
        check.eq('redeliver.duplicate', again.get('duplicate'), True)
        check.eq('redeliver.applied', r.inspect()['applied_lsn'], 4)
        # Different content at an accepted LSN -> explicit rejection.
        try:
            r.feed({'lsn': 2, 'kind': 'put', 'tx': 'seed', 'key': 'a',
                    'value': 999})
            check.ok('redeliver.conflict', False, 'not rejected')
        except ValueError as exc:
            check.eq('redeliver.conflict', str(exc), 'CONFLICTING_REDELIVERY')
        # bool is not an int value.
        r.feed({'lsn': 5, 'kind': 'begin', 'tx': 'z'})
        try:
            r.feed({'lsn': 6, 'kind': 'put', 'tx': 'z', 'key': 'q',
                    'value': True})
            check.ok('redeliver.bool', False, 'bool accepted as int')
        except ValueError as exc:
            check.eq('redeliver.bool', str(exc), 'BAD_VALUE')
        # The rejected record did not advance progress.
        check.eq('redeliver.bad_no_advance', r.inspect()['applied_lsn'], 5)
        # Reopen in the middle of tx 'z' and finish it.
        r.shutdown()
        r = Replay(path, grace=5)
        r.feed({'lsn': 6, 'kind': 'put', 'tx': 'z', 'key': 'q', 'value': 1})
        r.feed({'lsn': 7, 'kind': 'commit', 'tx': 'z'})
        ins = r.inspect()
        check.eq('reopen.visible', ins['visible_commit'], 7)
        check.eq('reopen.value', [v for v in ins['versions']
                                  if v['key'] == 'q' and v['dead'] is None],
                 [{'key': 'q', 'born': 7, 'dead': None, 'value': 1,
                   'deleted': 0}])
        r.shutdown()
    run_scenario('redelivery', body)


def test_reopen_waiting_cleanup_same_deadline(check):
    def body(path):
        r, q = Replay(path, grace=20), Readers(path)
        for rec in base_records():
            r.feed(rec)
        q.begin('pin')                 # snapshot 4
        commit = bump_b(r, 5, 60)      # commit at LSN 7 > snapshot
        r.feed({'lsn': 8, 'kind': 'cleanup', 'horizon': commit}, now=10)
        # Reopen BOTH services: the pending deadline and cursor survive.
        r.shutdown(); q.shutdown()
        r, q = Replay(path, grace=20), Readers(path)
        ins = r.inspect()
        check.eq('reopen.pending_deadline', ins['pending_cleanup']['deadline'], 30)
        check.eq('reopen.pending_applied', ins['applied_lsn'], commit)
        # Same cleanup after reopen: still the original deadline, not re-timed.
        res = r.feed({'lsn': 8, 'kind': 'cleanup', 'horizon': commit}, now=25)
        check.eq('reopen.pending_waiting', res['status'], 'WAITING')
        check.eq('reopen.pending_deadline2', res['deadline'], 30)
        res = r.feed({'lsn': 8, 'kind': 'cleanup', 'horizon': commit}, now=30)
        check.eq('reopen.pending_applied2', res['status'], 'APPLIED')
        check.eq('reopen.cancelled', res['cancelled'], ['pin'])
        check.eq('reopen.final_applied', r.inspect()['applied_lsn'], 8)
        r.shutdown(); q.shutdown()
    run_scenario('reopen_pending', body)


def test_cleanup_reclaims_versions(check):
    def body(path):
        r = Replay(path, grace=0),
        r = r[0]
        recs = base_records() + [
            {'lsn': 5, 'kind': 'begin', 'tx': 'u'},
            {'lsn': 6, 'kind': 'put', 'tx': 'u', 'key': 'a', 'value': 11},
            {'lsn': 7, 'kind': 'commit', 'tx': 'u'},
            {'lsn': 8, 'kind': 'begin', 'tx': 'v'},
            {'lsn': 9, 'kind': 'put', 'tx': 'v', 'key': 'a', 'value': 12},
            {'lsn': 10, 'kind': 'commit', 'tx': 'v'},
        ]
        for rec in recs:
            r.feed(rec)
        before = r.inspect()['versions']
        check.eq('reclaim.before_a_versions',
                 sorted((v['born'], v['dead']) for v in before if v['key'] == 'a'),
                 [(4, 7), (7, 10), (10, None)])
        r.feed({'lsn': 11, 'kind': 'cleanup', 'horizon': 10}, now=0)
        after = r.inspect()['versions']
        check.eq('reclaim.after_a_versions',
                 [(v['born'], v['dead']) for v in after if v['key'] == 'a'],
                 [(10, None)])
        # Live values and current deletion state are preserved.
        check.eq('reclaim.live', r.inspect()['visible_commit'], 10)
        r.shutdown()
    run_scenario('reclaim', body)


def test_plan_runner(check):
    from plan import run_plan
    with tempfile.TemporaryDirectory() as tmp:
        p = str(Path(tmp) / 'plan.sqlite')
        plan = {'grace': 5, 'steps': [
            {'op': 'feed', 'now': 0, 'record': {'lsn': 1, 'kind': 'begin', 'tx': 's'}},
            {'op': 'feed', 'now': 0, 'record': {'lsn': 2, 'kind': 'put', 'tx': 's', 'key': 'k', 'value': 1}},
            {'op': 'feed', 'now': 0, 'record': {'lsn': 3, 'kind': 'commit', 'tx': 's'}},
            {'op': 'begin', 'query_id': 'r'},              # pinned at snapshot 3
            {'op': 'next', 'query_id': 'r', 'n': 1},
            {'op': 'feed', 'now': 0, 'record': {'lsn': 4, 'kind': 'begin', 'tx': 'u'}},
            {'op': 'feed', 'now': 0, 'record': {'lsn': 5, 'kind': 'put', 'tx': 'u', 'key': 'k', 'value': 2}},
            {'op': 'feed', 'now': 0, 'record': {'lsn': 6, 'kind': 'commit', 'tx': 'u'}},
            {'op': 'reopen'},
            {'op': 'next', 'query_id': 'r', 'n': 1},
            {'op': 'feed', 'now': 1, 'record': {'lsn': 7, 'kind': 'cleanup', 'horizon': 6}},
            {'op': 'inspect'},
            {'op': 'next', 'query_id': 'r', 'n': 1},
            {'op': 'close', 'query_id': 'r'},
            {'op': 'continue_pending', 'now': 6},
            {'op': 'inspect'},
        ]}
        out = run_plan(plan, db_path=p)
        tr = out['trace']
        check.eq('plan.begin_snapshot', tr[3]['result']['snapshot'], 3)
        check.eq('plan.page_before_reopen', tr[4]['result'],
                 [{'key': 'k', 'value': 1}])
        check.eq('plan.reopen_ok', tr[8]['ok'], True)
        check.eq('plan.page_after_reopen', tr[9]['result'], [])
        check.eq('plan.cleanup_waiting', tr[10]['result']['status'], 'WAITING')
        check.eq('plan.applied_stuck', tr[11]['result']['applied_lsn'], 6)
        check.eq('plan.close', tr[13]['result']['state'], 'CLOSED')
        check.eq('plan.cleanup_applied', tr[14]['result']['status'], 'APPLIED')
        check.eq('plan.final_applied', tr[15]['result']['applied_lsn'], 7)


def main():
    check = Check()
    for name, fn in [
        ('interleaved', test_interleaved_and_snapshot),
        ('paging', test_paging_against_oracle),
        ('cleanup', test_cleanup_variants),
        ('redelivery', test_redelivery_and_reopen),
        ('reopen_pending', test_reopen_waiting_cleanup_same_deadline),
        ('reclaim', test_cleanup_reclaims_versions),
        ('plan', test_plan_runner),
    ]:
        try:
            fn(check)
        except Exception as exc:  # noqa: BLE001 - tests report all failures
            import traceback
            check.failures.append('%s: EXCEPTION %r\n%s'
                                  % (name, exc, traceback.format_exc()))
    if check.failures:
        print('FAIL (%d)' % len(check.failures))
        for failure in check.failures:
            print(' -', failure)
        return 1
    print('OK: all protocol checks passed')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
