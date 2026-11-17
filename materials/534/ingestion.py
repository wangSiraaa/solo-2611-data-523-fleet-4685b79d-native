import csv,sqlite3,io
class Ingestion:
    def __init__(self,registry,path):
        self.registry=registry;self.db=sqlite3.connect(path);self.db.execute('create table if not exists imported(shard text,row_no integer,value integer,source text)');self.calls=[]
    def read(self,source,shard,path):
        ticket=self.registry.acquire(source);self.calls.append({'source':source,'shard':shard,'state':self.registry.states[source]});raw=open(path,'rb').read()
        for n,row in enumerate(csv.DictReader(io.StringIO(raw.decode()))):self.db.execute('insert into imported values(?,?,?,?)',(shard,n,int(row['value']),source))
        self.db.commit();self.registry.release(ticket);return len(raw)
