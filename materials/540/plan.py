"""Replayable JSON operation plans.

A plan interleaves replication and report operations against one database:

    {"grace": 5, "path": "work.sqlite", "steps": [
       {"op": "feed",    "record": {"lsn": 1, "kind": "begin", "tx": "t"}, "now": 0},
       {"op": "begin",   "query_id": "r1"},
       {"op": "next",    "query_id": "r1", "n": 2},
       {"op": "status",  "query_id": "r1"},
       {"op": "close",   "query_id": "r1"},
       {"op": "continue_pending", "now": 10},
       {"op": "inspect"},
       {"op": "reopen"}
    ]}

Every step reports the *real* return value (or the error type and its
attributes) followed by an ``inspect`` snapshot of the standby.  The database
is a normal file, so running a plan twice against the same path demonstrates
reopen safety.

Usage:
    python3 plan.py plan.json [--db path] [--out result.json]
    python3 plan.py -              < plan.json
"""
import json
import sys

from readers import ConflictCancelled, Readers
from replay import Replay


def _run_step(session, step):
    op = step.get('op')
    if op == 'feed':
        return session['replay'].feed(step['record'], now=step.get('now', 0))
    if op == 'continue_pending':
        return session['replay'].continue_pending(now=step.get('now', 0))
    if op == 'begin':
        return session['readers'].begin(step.get('query_id', step.get('query')))
    if op == 'next':
        return session['readers'].next(step.get('query_id', step.get('query')),
                                       step.get('n', 10))
    if op == 'status':
        return session['readers'].status(step.get('query_id', step.get('query')))
    if op == 'close':
        return session['readers'].close(step.get('query_id', step.get('query')))
    if op == 'inspect':
        return session['replay'].inspect()
    raise ValueError('UNKNOWN_OP: %r' % (op,))


def _open(path, grace):
    return {'path': path, 'grace': grace,
            'replay': Replay(path, grace=grace),
            'readers': Readers(path)}


def _close(session):
    for key in ('replay', 'readers'):
        try:
            # Fold the WAL back into the main file so the saved database is
            # self-contained when the plan finishes.
            session[key].c.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        except Exception:
            pass
        try:
            session[key].shutdown()
        except Exception:  # already closed by an earlier reopen
            pass


def run_plan(plan, db_path=None):
    path = db_path or plan.get('path') or 'work.sqlite'
    grace = plan.get('grace', 5)
    session = _open(path, grace)
    trace = []
    try:
        for i, step in enumerate(plan.get('steps', [])):
            op = step.get('op')
            entry = {'step': i, 'op': op}
            if op == 'reopen':
                # Close every connection the plan currently holds, then open a
                # fresh pair against the same file.  ``inspect`` below is taken
                # through the *new* session, proving the state survived.
                _close(session)
                session = _open(path, grace)
                entry['ok'] = True
                entry['result'] = {'reopened': True, 'path': path,
                                   'grace': grace}
            else:
                try:
                    entry['ok'] = True
                    entry['result'] = _run_step(session, step)
                except ConflictCancelled as exc:
                    entry['ok'] = False
                    entry['error'] = 'ConflictCancelled'
                    entry['query_id'] = exc.query_id
                    entry['cancel_lsn'] = exc.cancel_lsn
                    entry['message'] = str(exc)
                except Exception as exc:  # surface the real failure in the trace
                    entry['ok'] = False
                    entry['error'] = type(exc).__name__
                    entry['message'] = str(exc)
            entry['inspect'] = session['replay'].inspect()
            trace.append(entry)
    finally:
        _close(session)
    return {'path': path, 'grace': grace, 'trace': trace}


def main(argv):
    if len(argv) < 2:
        print(__doc__)
        return 2
    if argv[1] == '-':
        plan = json.load(sys.stdin)
    else:
        with open(argv[1], encoding='utf-8') as fh:
            plan = json.load(fh)
    db_path = None
    out_path = None
    rest = argv[2:]
    for i in range(0, len(rest) - 1, 2):
        if rest[i] == '--db':
            db_path = rest[i + 1]
        elif rest[i] == '--out':
            out_path = rest[i + 1]
    out = run_plan(plan, db_path=db_path)
    text = json.dumps(out, ensure_ascii=False, indent=2)
    if out_path:
        with open(out_path, 'w', encoding='utf-8') as fh:
            fh.write(text + '\n')
    print(text)
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
