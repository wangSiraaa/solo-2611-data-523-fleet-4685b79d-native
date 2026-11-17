"""Small persistent row-version store used by both existing services."""
import sqlite3


def connect(path):
    c = sqlite3.connect(path)
    c.row_factory = sqlite3.Row
    c.executescript('''
    CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v INTEGER NOT NULL);
    INSERT OR IGNORE INTO meta VALUES('applied_lsn',0),('visible_commit',0);
    CREATE TABLE IF NOT EXISTS versions(
      key TEXT NOT NULL, born INTEGER NOT NULL, dead INTEGER,
      value INTEGER, deleted INTEGER NOT NULL, PRIMARY KEY(key,born));
    CREATE TABLE IF NOT EXISTS queries(
      id TEXT PRIMARY KEY, snapshot INTEGER NOT NULL, last_key TEXT,
      state TEXT NOT NULL, cancel_lsn INTEGER);
    ''')
    return c


def meta(c, key):
    return c.execute('SELECT v FROM meta WHERE k=?', (key,)).fetchone()[0]


def setmeta(c, key, value):
    c.execute('UPDATE meta SET v=? WHERE k=?', (value, key))


def visible(c, snapshot):
    return [dict(r) for r in c.execute('''SELECT key,value FROM versions
      WHERE born<=? AND (dead IS NULL OR dead>?) AND deleted=0
      ORDER BY CAST(key AS BLOB)''', (snapshot, snapshot))]


def write(c, key, value, deleted, version):
    c.execute('UPDATE versions SET dead=? WHERE key=? AND dead IS NULL', (version, key))
    c.execute('INSERT INTO versions VALUES(?,?,NULL,?,?)', (key,version,value,deleted))


def inspect(c):
    return {'applied_lsn':meta(c,'applied_lsn'), 'visible_commit':meta(c,'visible_commit'),
            'versions':[dict(r) for r in c.execute('SELECT * FROM versions ORDER BY key,born')],
            'queries':[dict(r) for r in c.execute('SELECT * FROM queries ORDER BY id')]}
