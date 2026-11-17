"""Interactive report cursors, separate from the replica feed client."""
from db import connect, meta, visible


class ConflictCancelled(RuntimeError):
    pass


class Readers:
    def __init__(self, path):
        self.c = connect(path)

    def begin(self, query_id):
        snapshot = meta(self.c,'visible_commit')
        self.c.execute('INSERT INTO queries VALUES(?,?,NULL,?,NULL)',(query_id,snapshot,'RUNNING'))
        self.c.commit()
        return {'query_id':query_id,'snapshot':snapshot}

    def next(self, query_id, n):
        q = self.status(query_id)
        if q['state'] != 'RUNNING':
            raise ValueError('QUERY_CLOSED')
        rows = visible(self.c,q['snapshot'])
        if q['last_key'] is not None:
            rows = [r for r in rows if r['key'].encode() > q['last_key'].encode()]
        page = rows[:n]
        if page:
            self.c.execute('UPDATE queries SET last_key=? WHERE id=?',(page[-1]['key'],query_id))
            self.c.commit()
        return page

    def status(self, query_id):
        q = self.c.execute('SELECT * FROM queries WHERE id=?',(query_id,)).fetchone()
        if q is None:
            raise ValueError('UNKNOWN_QUERY')
        return dict(q)

    def close(self, query_id):
        self.status(query_id)
        self.c.execute("UPDATE queries SET state='CLOSED' WHERE id=?",(query_id,))
        self.c.commit()
        return self.status(query_id)

    def shutdown(self):
        self.c.close()
