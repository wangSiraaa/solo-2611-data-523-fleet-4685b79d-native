import json,tempfile
from pathlib import Path
from branches import TableBranches
from retention import RetentionService
with tempfile.TemporaryDirectory() as td:
    p=Path(td);base=p/'base.jsonl';left=p/'rewrite.jsonl';right=p/'append.jsonl';orphan=p/'orphan.jsonl'
    for f,rows in [(base,[{'row_id':'a','value':1},{'row_id':'b','value':2}]),(left,[{'row_id':'a','value':10}]),(right,[{'row_id':'c','value':3}]),(orphan,[{'row_id':'z','value':9}])]:f.write_text(''.join(json.dumps(r)+'\n' for r in rows))
    b=TableBranches();b.create('main',[str(base)]);normal=b.read('main');b.fork('main','left');b.fork('main','right');b.rewrite('left',[str(base)],[str(left)]);b.append('right',right);b.merge('left','right');wrong=b.read('left')
    r=RetentionService(p,b);before=r.plan_gc();b.heads={k:v for k,v in b.heads.items() if k=='left'};b.heads['left']=[str(left),str(right)];after=r.plan_gc();deleted=r.gc()
    print(json.dumps({'normal':normal,'common_route_failure':{'merged_rows':wrong,'expected':[{'row_id':'a','value':10},{'row_id':'c','value':3}]},'coupling':{'before_reference_change':before,'after_reference_change':after,'actually_deleted':deleted,'remaining_rows':b.read('left')},'scope':'真实文件清单驱动read与gc；操作合并、历史/pin与发布保护均待实现。'},ensure_ascii=False))
