import json,hashlib
from pathlib import Path
class PagedSource:
    """Bounded source support only, never performs Federation/Budget policy."""
    def __init__(self,name,path,page_rows=1):
        self.name=name;self.path=Path(path);raw=self.path.read_bytes();self.snapshot=hashlib.sha256(raw).hexdigest();lines=raw.splitlines(keepends=True);self.pages=[];off=0
        for i in range(0,len(lines),page_rows):
            chunk=b''.join(lines[i:i+page_rows]);self.pages.append({'page_id':str(len(self.pages)),'offset':off,'upper_bound':len(chunk),'sha256':hashlib.sha256(chunk).hexdigest()});off+=len(chunk)
        self.receipts={};self.pending_resolution=set();self.audit=[]
    def catalog(self):return {'source':self.name,'snapshot':self.snapshot,'pages':[dict(p) for p in self.pages]}
    def ticket_id(self,query_id,page_id):return hashlib.sha256(json.dumps([self.name,self.snapshot,query_id,str(page_id)]).encode()).hexdigest()
    def scan(self,query_id,page_id,lose_response=False):
        page=next((p for p in self.pages if p['page_id']==str(page_id)),None)
        if page is None:raise ValueError('UNKNOWN_PAGE')
        tid=self.ticket_id(query_id,page_id)
        if tid not in self.receipts:
            with self.path.open('rb') as f:f.seek(page['offset']);raw=f.read(page['upper_bound'])
            if hashlib.sha256(raw).hexdigest()!=page['sha256']:raise ValueError('SOURCE_CHANGED')
            self.audit.append({'op':'scan','query_id':query_id,'page_id':str(page_id),'ticket_id':tid,'bytes':len(raw)})
            self.receipts[tid]={'status':'done','source':self.name,'snapshot':self.snapshot,'page_id':str(page_id),'ticket_id':tid,'scanned_bytes':len(raw),'rows':[json.loads(x) for x in raw.splitlines()]}
        if lose_response:self.pending_resolution.add(tid);raise TimeoutError('response lost after actual scan')
        return dict(self.receipts[tid])
    def get_ticket(self,query_id,page_id):
        tid=self.ticket_id(query_id,page_id)
        if tid in self.pending_resolution:return {'status':'unknown','ticket_id':tid}
        return dict(self.receipts[tid]) if tid in self.receipts else {'status':'not_started','ticket_id':tid}
    def resolve_for_test(self,query_id,page_id):self.pending_resolution.discard(self.ticket_id(query_id,page_id))
