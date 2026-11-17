import hashlib
def pointer(data):
    oid=hashlib.sha256(data).hexdigest()
    return ('version https://git-lfs.github.com/spec/v1\noid sha256:'+oid+'\nsize '+str(len(data))+'\n').encode()
class Objects:
    def __init__(self,data):self.data=data;self.oid=hashlib.sha256(data).hexdigest();self.downloads=0;self.now=0;self.actions={};self.mode='normal';self.events=[]
    def advance(self,seconds):self.now+=seconds
    def batch(self,oid):
        if oid!=self.oid:return {'error':404}
        ticket='ticket-'+str(len(self.actions)+1);expiry=self.now+5;self.actions[ticket]=expiry;self.events.append(['batch',ticket,expiry]);return {'ticket':ticket,'oid':oid,'size':len(self.data),'expires_at':expiry}
    def download(self,ticket):
        if ticket not in self.actions or self.now>=self.actions[ticket]:raise PermissionError('EXPIRED')
        self.downloads+=1;self.events.append(['download',ticket,self.mode])
        if self.mode=='disconnect':raise ConnectionError('DISCONNECTED')
        if self.mode=='short':return self.data[:len(self.data)//2]
        if self.mode=='corrupt':return b'X'*len(self.data)
        return self.data
