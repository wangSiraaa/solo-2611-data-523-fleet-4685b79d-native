from pathlib import Path
class RetentionService:
    def __init__(self,root,branches):self.root=Path(root);self.branches=branches
    def plan_gc(self):
        live={str(f) for fs in self.branches.heads.values() for f in fs};return sorted(str(p) for p in self.root.glob('*.jsonl') if str(p) not in live)
    def gc(self):
        victims=self.plan_gc()
        for p in victims:Path(p).unlink()
        return victims
