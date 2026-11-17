"""Persistent row-version store and helpers shared by both services.

On-disk shape (see README for the protocol rationale):

  versions       committed row versions.  ``born`` is inclusive and ``dead``
                 is exclusive; both are commit LSNs.  A deletion is a
                 tombstone row (``deleted=1``, ``value=NULL``).  An
                 uncommitted transaction never touches this table.
  txs / tx_base / tx_touched
                 the durable private view of an open transaction.  ``tx_base``
                 is the committed snapshot copied at begin time;
                 ``tx_touched`` holds the transaction's own final state per
                 key (or its deletion).
  records        one row per *accepted* WAL LSN with a canonical fingerprint
                 and the original result.  This makes redelivery idempotent
                 and conflicting redelivery detectable.
  pending        the single cleanup record waiting for its conflict set to
                 drain or for its fixed deadline.  ``applied_lsn`` is not
                 advanced while a row lives here.
  queries        reader registrations, snapshots, cursors and terminal state.

All multi-statement changes use explicit transactions so the two services,
which own independent connections, only ever see committed states.
"""
import json
import sqlite3
import threading
from contextlib import contextmanager


META_DEFAULTS = {
    'applied_lsn': 0,
    'visible_commit': 0,
    'last_horizon': 0,
    'last_now': 0,
}


class _Conn(sqlite3.Connection):
    """Connection subclass so we can hang a per-instance transaction lock."""
    _tx_lock = None


def connect(path):
    """Open one independent connection and make sure the schema exists.

    The connection may be shared by a few threads inside one process (the
    harness exercises concurrent report/feed calls).  SQLite is serialised per
    connection by the RLock below, and cross-connection visibility is handled
    by WAL plus BEGIN IMMEDIATE.
    """
    c = sqlite3.connect(str(path), timeout=30.0, isolation_level=None,
                        check_same_thread=False, factory=_Conn)
    c.row_factory = sqlite3.Row
    c._tx_lock = threading.RLock()
    # WAL lets the report connection read a stable snapshot while the replay
    # connection publishes a commit.  FULL makes a returned feed durable.
    c.execute('PRAGMA journal_mode=WAL')
    c.execute('PRAGMA synchronous=FULL')
    c.execute('PRAGMA busy_timeout=30000')
    c.executescript('''
    CREATE TABLE IF NOT EXISTS meta(
      k TEXT PRIMARY KEY, v INTEGER NOT NULL);

    CREATE TABLE IF NOT EXISTS versions(
      key TEXT NOT NULL, born INTEGER NOT NULL, dead INTEGER,
      value INTEGER, deleted INTEGER NOT NULL,
      PRIMARY KEY(key,born));

    CREATE TABLE IF NOT EXISTS queries(
      id TEXT PRIMARY KEY, snapshot INTEGER NOT NULL, last_key TEXT,
      state TEXT NOT NULL, cancel_lsn INTEGER, cancel_reason TEXT);

    CREATE TABLE IF NOT EXISTS records(
      lsn INTEGER PRIMARY KEY, fingerprint TEXT NOT NULL,
      kind TEXT NOT NULL, result TEXT NOT NULL);

    CREATE TABLE IF NOT EXISTS pending(
      lsn INTEGER PRIMARY KEY, kind TEXT NOT NULL, horizon INTEGER NOT NULL,
      deadline INTEGER NOT NULL, fingerprint TEXT NOT NULL,
      created_now INTEGER NOT NULL);

    CREATE TABLE IF NOT EXISTS txs(
      tx TEXT PRIMARY KEY, begin_snapshot INTEGER NOT NULL);

    CREATE TABLE IF NOT EXISTS tx_base(
      tx TEXT NOT NULL, key TEXT NOT NULL, value INTEGER,
      PRIMARY KEY(tx,key));

    CREATE TABLE IF NOT EXISTS tx_touched(
      tx TEXT NOT NULL, key TEXT NOT NULL, value INTEGER,
      deleted INTEGER NOT NULL, PRIMARY KEY(tx,key));

    CREATE TABLE IF NOT EXISTS commits(
      lsn INTEGER PRIMARY KEY, tx TEXT NOT NULL);

    CREATE INDEX IF NOT EXISTS versions_dead_idx ON versions(dead);
    CREATE INDEX IF NOT EXISTS queries_state_idx ON queries(state,snapshot);
    ''')
    for k, v in META_DEFAULTS.items():
        c.execute('INSERT OR IGNORE INTO meta(k,v) VALUES(?,?)', (k, v))
    # Migration from the starting schema, which had no cancel_reason.
    cols = {r['name'] for r in c.execute('PRAGMA table_info(queries)')}
    if 'cancel_reason' not in cols:
        c.execute('ALTER TABLE queries ADD COLUMN cancel_reason TEXT')
    return c


@contextmanager
def tx(c, immediate=True):
    """Explicit transaction; helpers are safe to call from inside it.

    The connection-level RLock is held for the entire block, so concurrent
    threads sharing one service instance never interleave their statements
    inside a transaction.
    """
    lock = getattr(c, '_tx_lock', None)
    if lock is not None:
        lock.acquire()
    try:
        if c.in_transaction:
            yield
            return
        c.execute('BEGIN IMMEDIATE' if immediate else 'BEGIN')
        try:
            yield
            c.execute('COMMIT')
        except BaseException:
            try:
                c.execute('ROLLBACK')
            except sqlite3.Error:
                pass
            raise
    finally:
        if lock is not None:
            lock.release()


def meta(c, key):
    row = c.execute('SELECT v FROM meta WHERE k=?', (key,)).fetchone()
    return META_DEFAULTS[key] if row is None else row[0]


def setmeta(c, key, value):
    c.execute('INSERT INTO meta(k,v) VALUES(?,?) '
              'ON CONFLICT(k) DO UPDATE SET v=excluded.v', (key, int(value)))


def fingerprint(record):
    """Canonical content identity, including int-vs-bool distinctions."""
    try:
        return json.dumps(record, sort_keys=True, separators=(',', ':'),
                          ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError('BAD_RECORD: not JSON-serialisable: %s' % exc)


def visible(c, snapshot):
    """All live committed rows visible at ``snapshot`` in UTF-8 byte order."""
    return [dict(r) for r in c.execute('''
      SELECT key,value FROM versions
      WHERE born<=? AND (dead IS NULL OR dead>?) AND deleted=0
      ORDER BY CAST(key AS BLOB)''', (snapshot, snapshot))]


def page(c, snapshot, after_key=None, limit=None):
    """One page of the view at ``snapshot`` strictly after ``after_key``."""
    sql = ('SELECT key,value FROM versions '
           'WHERE born<=? AND (dead IS NULL OR dead>?) AND deleted=0')
    args = [snapshot, snapshot]
    if after_key is not None:
        sql += ' AND CAST(key AS BLOB) > CAST(? AS BLOB)'
        args.append(after_key)
    sql += ' ORDER BY CAST(key AS BLOB)'
    if limit is not None:
        sql += ' LIMIT ?'
        args.append(limit)
    return [dict(r) for r in c.execute(sql, args)]


def write(c, key, value, deleted, version):
    """Publish one new version, ending the currently live version."""
    c.execute('UPDATE versions SET dead=? WHERE key=? AND dead IS NULL',
              (version, key))
    c.execute('INSERT INTO versions(key,born,dead,value,deleted) '
              'VALUES(?,?,NULL,?,?)',
              (key, version, value, 1 if deleted else 0))


def get_pending(c):
    row = c.execute('SELECT * FROM pending LIMIT 1').fetchone()
    return None if row is None else dict(row)


def inspect(c):
    """Coherent snapshot of everything observable on the standby."""
    with tx(c, immediate=False):
        pending = get_pending(c)
        pending_list = []
        if pending is not None:
            pending_list.append({
                'lsn': pending['lsn'],
                'kind': pending['kind'],
                'horizon': pending['horizon'],
                'deadline': pending['deadline'],
                'status': 'WAITING',
            })
        return {
            'applied_lsn': meta(c, 'applied_lsn'),
            'visible_commit': meta(c, 'visible_commit'),
            'last_horizon': meta(c, 'last_horizon'),
            'versions': [dict(r) for r in c.execute(
                'SELECT key,born,dead,value,deleted FROM versions '
                'ORDER BY CAST(key AS BLOB),born')],
            'queries': [dict(r) for r in c.execute(
                'SELECT id,snapshot,last_key,state,cancel_lsn,cancel_reason '
                'FROM queries ORDER BY id')],
            'pending': pending_list,
            # Convenient single-record alias; both describe the same state.
            'pending_cleanup': None if pending is None else {
                'lsn': pending['lsn'],
                'horizon': pending['horizon'],
                'deadline': pending['deadline'],
            },
        }
