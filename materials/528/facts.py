import json,sqlite3
class Facts:
    def __init__(self,path):
        self.db=sqlite3.connect(path);self.db.execute('create table if not exists events(seq integer primary key,event_id text,meter text,a text,b text,amount integer)');self.db.commit()
    def append(self,event_id,meter,a,b,amount):
        self.db.execute('insert into events(event_id,meter,a,b,amount) values(?,?,?,?,?)',(event_id,meter,a,b,amount));self.db.commit();return self.db.execute('select max(seq) from events').fetchone()[0]
    def current(self,meter):
        row=self.db.execute('select amount from events where meter=? order by seq desc limit 1',(meter,)).fetchone();return 0 if row is None else row[0]
