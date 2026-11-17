import json
class Pages:
    def __init__(self,samples):self.samples=samples
    def get(self,path):
        if path=='/account/delta/t1':return self.samples['pages'][0]
        if path=='/account/delta/p1':return self.samples['pages'][1]
        if path=='/account/delta/p2':return self.samples['pages'][2]
        raise KeyError('UNKNOWN_LINK')
