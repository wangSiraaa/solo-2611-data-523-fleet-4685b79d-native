"""Interactive report cursors on an independent SQLite connection.

A cursor pins the ``visible_commit`` observed at ``begin`` for its whole life.
Commit LSNs are monotonic, so a view at snapshot S never changes and paging is
a plain byte-ordered scan of versions with ``born <= S < dead``.  The cursor
position (``last_key``) is durable, so a reopened process resumes exactly where
the previous one stopped.

A cleanup reaches into this connection only through the ``queries`` table.  It
may flip a still-running cursor at/under its horizon to ``CANCELLED`` at the
deadline; from then on ``next`` raises :class:`ConflictCancelled`.  ``close``
never rewrites a CANCELLED terminal state into a successful completion.
"""
from db import connect, meta, page, tx


class ConflictCancelled(RuntimeError):
    """Raised by ``next`` after a cleanup cancelled the query."""

    def __init__(self, query_id, lsn):
        self.query_id = query_id
        self.cancel_lsn = lsn
        super().__init__('ConflictCancelled: query %r cancelled by cleanup lsn %r'
                         % (query_id, lsn))


class Readers:
    def __init__(self, path):
        self.c = connect(path)

    def begin(self, query_id):
        if not isinstance(query_id, str) or query_id == '':
            raise ValueError('BAD_QUERY_ID')
        with tx(self.c):
            exists = self.c.execute('SELECT 1 FROM queries WHERE id=?',
                                    (query_id,)).fetchone()
            if exists is not None:
                raise ValueError('QUERY_EXISTS')
            # Read the commit under the same write transaction that registers
            # the query, so a concurrent publish is either wholly before or
            # wholly after this snapshot.
            snapshot = meta(self.c, 'visible_commit')
            self.c.execute(
                "INSERT INTO queries(id,snapshot,last_key,state,cancel_lsn,"
                "cancel_reason) VALUES(?,?,NULL,'RUNNING',NULL,NULL)",
                (query_id, snapshot))
        return {'query_id': query_id, 'snapshot': snapshot, 'state': 'RUNNING'}

    def next(self, query_id, n):
        if isinstance(n, bool) or not isinstance(n, int) or n < 0:
            raise ValueError('BAD_PAGE_SIZE')
        with tx(self.c):
            q = self._row(query_id)
            if q['state'] == 'CANCELLED':
                raise ConflictCancelled(query_id, q['cancel_lsn'])
            if q['state'] != 'RUNNING':
                raise ValueError('QUERY_CLOSED')
            if n == 0:
                return []
            rows = page(self.c, q['snapshot'], q['last_key'], n)
            if rows:
                self.c.execute('UPDATE queries SET last_key=? WHERE id=?',
                               (rows[-1]['key'], query_id))
            return rows

    def status(self, query_id):
        with tx(self.c, immediate=False):
            return dict(self._row(query_id))

    def close(self, query_id):
        with tx(self.c):
            q = self._row(query_id)
            if q['state'] == 'RUNNING':
                self.c.execute("UPDATE queries SET state='CLOSED' WHERE id=?",
                               (query_id,))
                q = self._row(query_id)
            # CLOSED stays CLOSED (idempotent); CANCELLED stays CANCELLED.
            return dict(q)

    def shutdown(self):
        self.c.close()

    # ---------------------------------------------------------------- internal

    def _row(self, query_id):
        q = self.c.execute('SELECT * FROM queries WHERE id=?',
                           (query_id,)).fetchone()
        if q is None:
            raise ValueError('UNKNOWN_QUERY')
        return q
