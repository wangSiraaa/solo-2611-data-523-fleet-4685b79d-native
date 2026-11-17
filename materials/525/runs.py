import json, sqlite3, uuid
class RunDirectory:
    def __init__(self,path):
        self.db=sqlite3.connect(path)
        self.db.execute('create table if not exists runs(id text primary key,model text,a text,b text,state text,result text)');self.db.commit()
    def submit(self,model,a,b):
        rid=uuid.uuid4().hex
        self.db.execute('insert into runs values(?,?,?,?,?,?)',(rid,model,a,b,'queued',None));self.db.commit();return rid
    def finish(self,rid,result):
        self.db.execute("update runs set state='success',result=? where id=?",(json.dumps(result),rid));self.db.commit()
    def success(self,model,a,b):
        r=self.db.execute("select id,result from runs where model=? and a=? and b=? and state='success' order by rowid desc limit 1",(model,a,b)).fetchone()
        return None if not r else {'id':r[0],'result':json.loads(r[1])}
    def ready(self,requirements):
        return all(self.success(*r) is not None for r in requirements)
    def run_extract(self,rid,input_path,a,b,output_path):
        rows=[r for r in map(json.loads,open(input_path)) if a<=r['date']<b]
        result={'sum':sum(r['amount'] for r in rows),'source_ids':[r['id'] for r in rows]}
        open(output_path,'w').write(json.dumps(result));self.finish(rid,result);return result
