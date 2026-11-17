class SourceRegistry:
    def __init__(self):self.states={'A':'OPEN','B':'OPEN'};self.refs={'A':0,'B':0}
    def acquire(self,source):
        if self.states[source]=='CLOSED':raise RuntimeError('CLOSED')
        self.refs[source]+=1;return {'source':source}
    def maintain(self,source):self.states[source]='DRAINING'
    def release(self,ticket):self.refs[ticket['source']]-=1
    def finish_maintenance(self,source):
        if self.refs[source]:raise RuntimeError('IN_FLIGHT')
        self.states[source]='CLOSED'
