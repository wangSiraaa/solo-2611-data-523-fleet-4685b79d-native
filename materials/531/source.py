import json,hashlib
class Source:
    def __init__(self,path):self.path=path;self.receipts={};self.scan_count=0
    def upper_bound(self):return len(open(self.path,'rb').read())
    def scan(self,query_id,lose_response=False):
        ticket=hashlib.sha256((query_id+'|'+str(self.path)).encode()).hexdigest()
        if ticket not in self.receipts:
            raw=open(self.path,'rb').read();self.scan_count+=1;self.receipts[ticket]={'ticket_id':ticket,'scanned_bytes':len(raw),'rows':[json.loads(x) for x in raw.splitlines()]}
        if lose_response:raise TimeoutError('response lost after scan')
        return dict(self.receipts[ticket])
