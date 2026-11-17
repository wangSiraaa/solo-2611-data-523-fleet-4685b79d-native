import json
class TableBranches:
    def __init__(self):self.heads={};self.snapshots={}
    def create(self,branch,files):self.heads[branch]=list(files);return list(files)
    def fork(self,source,target):self.heads[target]=list(self.heads[source])
    def append(self,branch,file):self.heads[branch].append(str(file))
    def rewrite(self,branch,old,new):self.heads[branch]=[f for f in self.heads[branch] if f not in old]+list(new)
    def merge(self,target,source):self.heads[target]=sorted(set(self.heads[target]+self.heads[source]));return list(self.heads[target])
    def read(self,branch):
        return [json.loads(line) for f in self.heads[branch] for line in open(f)]
