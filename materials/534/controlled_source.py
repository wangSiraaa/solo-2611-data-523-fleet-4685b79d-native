import threading,hashlib
class ControlledSource:
    def __init__(self,name,path):
        self.name=name;self.path=path;self.active=0;self.closed=False;self.lock=threading.Lock();self.before_read=threading.Event();self.allow_read=threading.Event();self.allow_read.set();self.before_callback=threading.Event();self.allow_callback=threading.Event();self.allow_callback.set();self.audit=[]
    def descriptor(self):
        raw=open(self.path,'rb').read();return {'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'snapshot':'frozen-1'}
    def read(self):
        with self.lock:
            if self.closed:raise RuntimeError('SOURCE_CLOSED')
            self.active+=1;self.audit.append({'op':'read-start','active':self.active})
        self.before_read.set();self.allow_read.wait(10)
        try:
            raw=open(self.path,'rb').read();self.before_callback.set();self.allow_callback.wait(10);return raw
        finally:
            with self.lock:self.active-=1;self.audit.append({'op':'read-end','active':self.active})
    def close(self):
        with self.lock:
            if self.active:raise RuntimeError('ACTIVE_READS')
            self.closed=True;self.audit.append({'op':'close','active':0})
