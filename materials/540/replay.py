"""Standby WAL replay service.

Protocol in one place:

* Input is an ordered stream of JSON records with ``lsn = 1,2,...``.
  A gap or an out-of-order record is rejected (``LSN_GAP``); an accepted
  record advances ``applied_lsn`` by exactly one.
* ``put``/``delete`` mutate only the transaction's private view
  (``tx_base`` + ``tx_touched``).  Nothing is visible to report clients until
  ``commit`` publishes every touched key atomically under that commit LSN.
* A ``cleanup`` is either applied immediately (no conflicting cursor) or parked
  in ``pending`` with a deadline fixed on *first* receipt.  While parked,
  ``applied_lsn`` stops in front of it, so later LSNs cannot overtake it.
* Every accepted non-pending record is stored in ``records`` with a canonical
  fingerprint and its original result.  Redelivery of identical content returns
  that result; redelivery of different content at an accepted LSN raises
  ``CONFLICTING_REDELIVERY``.
"""
import json
import threading

from db import (connect, fingerprint, get_pending, inspect, meta, setmeta,
                tx, write)

INT64_MIN = -(2 ** 63)
INT64_MAX = 2 ** 63 - 1


class Replay:
    def __init__(self, path, grace=5):
        if isinstance(grace, bool) or not isinstance(grace, int) or grace < 0:
            raise ValueError('BAD_GRACE')
        self.c = connect(path)
        self.grace = grace
        # One replication client drives one stream; serialise whole feed calls
        # so the clock step and the record step cannot interleave.
        self._lock = threading.RLock()

    # ------------------------------------------------------------------ public

    def feed(self, record, now=0):
        with self._lock:
            return self._feed(record, now)

    def _feed(self, record, now):
        if isinstance(now, bool) or not isinstance(now, int) or now < 0:
            raise ValueError('BAD_NOW')
        lsn, kind = self._validate_envelope(record)
        fp = fingerprint(record)

        # Advance the logical clock monotonically even if the record itself is
        # rejected.  Reading it back from the database makes a reopened (or
        # concurrent) instance observe the same, never-decreasing time.
        effective_now = self._advance_clock(now)

        with tx(self.c):
            applied = meta(self.c, 'applied_lsn')

            # -- exact redelivery of an already accepted LSN -----------------
            if lsn <= applied:
                return self._replay_stored(lsn, fp)

            pending = get_pending(self.c)
            if pending is not None:
                if lsn == pending['lsn']:
                    # Same cleanup arriving again: never re-time its deadline.
                    if pending['fingerprint'] != fp:
                        raise ValueError('CONFLICTING_REDELIVERY')
                    return self._strip(self._drive_pending(effective_now))
                if lsn > pending['lsn']:
                    raise ValueError('CLEANUP_PENDING')
                # lsn < pending.lsn implies it was accepted, handled above.
                raise ValueError('CLEANUP_PENDING')

            if lsn != applied + 1:
                raise ValueError('LSN_GAP')

            handler = getattr(self, '_on_' + kind)
            result = handler(record, lsn, effective_now)

            # Cleanup drives its own durable state: WAITING advances nothing,
            # and an applied cleanup has already stored the record and set
            # applied_lsn inside the same transaction as the cancellation.
            if result.pop('_self_committed', False):
                return result
            if result.get('status') == 'APPLIED':
                self._store(lsn, fp, kind, result)
                setmeta(self.c, 'applied_lsn', lsn)
            return result

    def continue_pending(self, now):
        """Drive a parked cleanup using new logical time (optional helper)."""
        with self._lock:
            if isinstance(now, bool) or not isinstance(now, int) or now < 0:
                raise ValueError('BAD_NOW')
            effective_now = self._advance_clock(now)
            with tx(self.c):
                pending = get_pending(self.c)
                if pending is None:
                    return {'status': 'IDLE', 'lsn': meta(self.c, 'applied_lsn')}
                return self._strip(self._drive_pending(effective_now))

    def _strip(self, result):
        result.pop('_self_committed', None)
        return result

    def inspect(self):
        return inspect(self.c)

    def shutdown(self):
        self.c.close()

    # --------------------------------------------------------------- handlers

    def _on_begin(self, record, lsn, now):
        txid = self._require_tx(record)
        if self._open(txid):
            raise ValueError('TX_ALREADY_OPEN')
        snapshot = meta(self.c, 'visible_commit')
        self.c.execute('INSERT INTO txs(tx,begin_snapshot) VALUES(?,?)',
                       (txid, snapshot))
        self.c.execute(
            'INSERT INTO tx_base(tx,key,value) '
            'SELECT ?,key,value FROM versions '
            'WHERE born<=? AND (dead IS NULL OR dead>?) AND deleted=0',
            (txid, snapshot, snapshot))
        return {'status': 'APPLIED', 'lsn': lsn, 'tx': txid, 'kind': 'begin'}

    def _on_put(self, record, lsn, now):
        txid = self._require_open_tx(record)
        key = self._require_key(record)
        value = self._require_value(record, 'value')
        # put is a plain assignment; repeated puts on one key are read in
        # source order and the last one wins.
        self.c.execute(
            'INSERT INTO tx_touched(tx,key,value,deleted) VALUES(?,?,?,0) '
            'ON CONFLICT(tx,key) DO UPDATE SET value=excluded.value,deleted=0',
            (txid, key, value))
        return {'status': 'APPLIED', 'lsn': lsn, 'kind': 'put', 'tx': txid,
                'key': key, 'value': value}

    def _on_delete(self, record, lsn, now):
        txid = self._require_open_tx(record)
        key = self._require_key(record)
        if self._private_value(txid, key) is None:
            # delete must remove a key that exists in the transaction's view.
            raise ValueError('DELETE_MISSING_KEY')
        self.c.execute(
            'INSERT INTO tx_touched(tx,key,value,deleted) VALUES(?,?,NULL,1) '
            'ON CONFLICT(tx,key) DO UPDATE SET value=NULL,deleted=1',
            (txid, key))
        return {'status': 'APPLIED', 'lsn': lsn, 'kind': 'delete', 'tx': txid,
                'key': key}

    def _on_commit(self, record, lsn, now):
        txid = self._require_open_tx(record)
        # Publish every touched key atomically, identified by this commit LSN.
        for row in self.c.execute(
                'SELECT key,value,deleted FROM tx_touched WHERE tx=? ORDER BY key',
                (txid,)).fetchall():
            write(self.c, row['key'], row['value'], row['deleted'], lsn)
        self.c.execute('DELETE FROM tx_touched WHERE tx=?', (txid,))
        self.c.execute('DELETE FROM tx_base WHERE tx=?', (txid,))
        self.c.execute('DELETE FROM txs WHERE tx=?', (txid,))
        self.c.execute('INSERT OR REPLACE INTO commits(lsn,tx) VALUES(?,?)',
                       (lsn, txid))
        setmeta(self.c, 'visible_commit', lsn)
        return {'status': 'APPLIED', 'lsn': lsn, 'kind': 'commit',
                'tx': txid, 'visible_commit': lsn}

    def _on_abort(self, record, lsn, now):
        txid = self._require_open_tx(record)
        self.c.execute('DELETE FROM tx_touched WHERE tx=?', (txid,))
        self.c.execute('DELETE FROM tx_base WHERE tx=?', (txid,))
        self.c.execute('DELETE FROM txs WHERE tx=?', (txid,))
        return {'status': 'APPLIED', 'lsn': lsn, 'kind': 'abort', 'tx': txid}

    def _on_cleanup(self, record, lsn, now):
        horizon = record.get('horizon')
        if isinstance(horizon, bool) or not isinstance(horizon, int) or horizon < 1:
            raise ValueError('BAD_HORIZON')
        if self.c.execute('SELECT 1 FROM commits WHERE lsn=?',
                          (horizon,)).fetchone() is None:
            raise ValueError('HORIZON_NOT_COMMITTED')
        if horizon < meta(self.c, 'last_horizon'):
            raise ValueError('HORIZON_REGRESSION')

        deadline = now + self.grace
        self.c.execute(
            'INSERT INTO pending(lsn,kind,horizon,deadline,fingerprint,created_now) '
            'VALUES(?,?,?,?,?,?)',
            (lsn, 'cleanup', horizon, deadline, fingerprint(record), now))
        return self._drive_pending(now)

    # ---------------------------------------------------------------- internal

    def _advance_clock(self, now):
        """Latch the logical clock at ``now`` (never backwards) and return it."""
        with tx(self.c):
            effective = max(now, meta(self.c, 'last_now'))
            setmeta(self.c, 'last_now', effective)
        return effective

    def _replay_stored(self, lsn, fp):
        row = self.c.execute(
            'SELECT fingerprint,result FROM records WHERE lsn=?', (lsn,)).fetchone()
        if row is None:
            # No recorded content (e.g. rows written by the starting version).
            # It is still behind applied_lsn, so treat it as already accepted.
            return {'status': 'APPLIED', 'lsn': lsn, 'duplicate': True}
        if row['fingerprint'] != fp:
            raise ValueError('CONFLICTING_REDELIVERY')
        result = json.loads(row['result'])
        result['duplicate'] = True
        return result

    def _store(self, lsn, fp, kind, result):
        self.c.execute(
            'INSERT INTO records(lsn,fingerprint,kind,result) VALUES(?,?,?,?)',
            (lsn, fp, kind, json.dumps(result, sort_keys=True,
                                       ensure_ascii=False)))

    def _drive_pending(self, now):
        pending = get_pending(self.c)
        if pending is None:
            return {'status': 'IDLE', 'lsn': meta(self.c, 'applied_lsn')}

        lsn = pending['lsn']
        horizon = pending['horizon']
        conflicts = self.c.execute(
            "SELECT id,snapshot FROM queries "
            "WHERE state='RUNNING' AND snapshot<? ORDER BY id",
            (horizon,)).fetchall()

        if conflicts and now < pending['deadline']:
            return {'status': 'WAITING', 'lsn': lsn, 'kind': 'cleanup',
                    'horizon': horizon, 'deadline': pending['deadline'],
                    'conflicts': [dict(r) for r in conflicts]}

        cancelled = []
        if conflicts:
            # Deadline reached: cancel first, in the very same transaction as
            # the cleanup publication, so no state can see "cleaned but a
            # cancelled cursor still runs".
            for row in conflicts:
                self.c.execute(
                    "UPDATE queries SET state='CANCELLED', cancel_lsn=?, "
                    "cancel_reason='DEADLINE' "
                    "WHERE id=? AND state='RUNNING'",
                    (lsn, row['id']))
                cancelled.append(row['id'])

        # Safe now: every still-active snapshot is >= horizon, so versions
        # with dead <= horizon are invisible to all of them.
        self.c.execute('DELETE FROM versions WHERE dead IS NOT NULL AND dead<=?',
                       (horizon,))
        setmeta(self.c, 'last_horizon', horizon)
        self.c.execute('DELETE FROM pending WHERE lsn=?', (lsn,))

        result = {'status': 'APPLIED', 'lsn': lsn, 'kind': 'cleanup',
                  'horizon': horizon, 'deadline': pending['deadline']}
        if cancelled:
            result['cancelled'] = cancelled
        self._store(lsn, pending['fingerprint'], 'cleanup', result)
        setmeta(self.c, 'applied_lsn', lsn)
        result['_self_committed'] = True
        return result

    def _private_value(self, txid, key):
        row = self.c.execute(
            'SELECT value,deleted FROM tx_touched WHERE tx=? AND key=?',
            (txid, key)).fetchone()
        if row is not None:
            return None if row['deleted'] else row['value']
        row = self.c.execute(
            'SELECT value FROM tx_base WHERE tx=? AND key=?', (txid, key)).fetchone()
        return None if row is None else row['value']

    def _open(self, txid):
        return self.c.execute('SELECT 1 FROM txs WHERE tx=?',
                              (txid,)).fetchone() is not None

    # -------------------------------------------------------------- validation

    def _validate_envelope(self, record):
        if not isinstance(record, dict):
            raise ValueError('BAD_RECORD')
        lsn = record.get('lsn')
        if isinstance(lsn, bool) or not isinstance(lsn, int) or lsn < 1:
            raise ValueError('BAD_LSN')
        kind = record.get('kind')
        if kind not in ('begin', 'put', 'delete', 'commit', 'abort', 'cleanup'):
            raise ValueError('UNKNOWN_RECORD')
        return lsn, kind

    def _require_tx(self, record):
        txid = record.get('tx')
        if not isinstance(txid, str) or txid == '':
            raise ValueError('BAD_TX')
        return txid

    def _require_open_tx(self, record):
        txid = self._require_tx(record)
        if not self._open(txid):
            raise ValueError('UNKNOWN_TX')
        return txid

    def _require_key(self, record):
        key = record.get('key')
        if not isinstance(key, str) or key == '':
            raise ValueError('BAD_KEY')
        return key

    def _require_value(self, record, field):
        value = record.get(field)
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError('BAD_VALUE')
        if not (INT64_MIN <= value <= INT64_MAX):
            raise ValueError('BAD_VALUE')
        return value
