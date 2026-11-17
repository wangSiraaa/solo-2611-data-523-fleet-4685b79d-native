import json
class JobService:
    def __init__(self,catalog):self.catalog=catalog;self.jobs={}
    def submit(self,job_id,namespace,name):
        self.catalog.resolve(namespace,name);self.jobs[job_id]={'namespace':namespace,'name':name}
    def execute(self,job_id):
        j=self.jobs[job_id];obj=self.catalog.resolve(j['namespace'],j['name'])
        rows=[json.loads(x) for x in open(obj['path'])]
        return {'dataset_id':obj['dataset_id'],'snapshot_id':obj['snapshot_id'],'sum':sum(r['amount'] for r in rows),'row_ids':[r['id'] for r in rows]}
