"""Current streaming redo API; single uncontentious transactions are usable."""
from db import connect, meta, setmeta, write, inspect


class Replay:
    def __init__(self, path, grace=5):
        self.c = connect(path)
        self.grace = grace
        self.open_tx = set()

    def feed(self, record, now=0):
        lsn, kind = record['lsn'], record['kind']
        if lsn <= meta(self.c,'applied_lsn'):
            return {'status':'APPLIED','lsn':lsn}
        if lsn != meta(self.c,'applied_lsn') + 1:
            raise ValueError('LSN_GAP')
        if kind == 'begin':
            self.open_tx.add(record['tx'])
        elif kind in ('put','delete'):
            if record['tx'] not in self.open_tx:
                raise ValueError('UNKNOWN_TX')
            write(self.c,record['key'],record.get('value'),kind=='delete',lsn)
        elif kind in ('commit','abort'):
            self.open_tx.remove(record['tx'])
            if kind == 'commit':
                setmeta(self.c,'visible_commit',lsn)
        elif kind == 'cleanup':
            self.c.execute('DELETE FROM versions WHERE dead<=?',(record['horizon'],))
        else:
            raise ValueError('UNKNOWN_RECORD')
        setmeta(self.c,'applied_lsn',lsn)
        self.c.commit()
        return {'status':'APPLIED','lsn':lsn}

    def inspect(self):
        return inspect(self.c)

    def shutdown(self):
        self.c.close()
