import json,uuid,hashlib
class Catalog:
    def __init__(self):self.objects={};self.names={}
    def create(self,namespace,name,path):
        k=(namespace,name)
        if k in self.names:raise ValueError('NAME_EXISTS')
        oid=uuid.uuid4().hex;raw=open(path,'rb').read();self.objects[oid]={'dataset_id':oid,'snapshot_id':uuid.uuid4().hex,'path':str(path),'sha256':hashlib.sha256(raw).hexdigest()};self.names[k]=oid;return oid
    def resolve(self,namespace,name):return dict(self.objects[self.names[(namespace,name)]])
    def move(self,old,new):
        if new in self.names:raise ValueError('NAME_EXISTS')
        self.names[new]=self.names.pop(old)
