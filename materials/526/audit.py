import sqlite3,json
class AuditSession:
    def __init__(self,connection):self.db=connection;self.events=[]
    def execute(self,sql,params=()):
        c=self.db.execute(sql,params);event={'sql':sql,'params':list(params),'lastrowid':c.lastrowid,'rowcount':c.rowcount}
        self.events.append(event);return c
    def export(self,path):open(path,'w').write(json.dumps(self.events,ensure_ascii=False))
def schema(c):
    c.executescript('create table customers(id integer primary key autoincrement,alias text unique,password text);create table orders(id integer primary key,customer_id integer references customers(id),amount integer,external_ref text);');c.execute('pragma foreign_keys=on')
