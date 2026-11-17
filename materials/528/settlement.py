import sqlite3
class Settlement:
    def __init__(self,path,facts):
        self.facts=facts;self.db=sqlite3.connect(path);self.db.execute('create table if not exists bills(month text,meter text,amount integer)');self.db.execute('create table if not exists adjustments(month text,meter text,amount integer)');self.db.commit()
    def seal(self,month,meter):
        v=self.facts.current(meter);self.db.execute('insert into bills values(?,?,?)',(month,meter,v));self.db.commit();return v
    def reconcile(self,month,meter):
        original=self.db.execute('select amount from bills where month=? and meter=?',(month,meter)).fetchone()[0];delta=self.facts.current(meter)-original
        self.db.execute('insert into adjustments values(?,?,?)',(month,meter,delta));self.db.commit();return delta
    def posted(self,month,meter):
        return self.db.execute('select coalesce(sum(amount),0) from adjustments where month=? and meter=?',(month,meter)).fetchone()[0]
