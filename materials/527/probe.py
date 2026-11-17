import tempfile,json
from pathlib import Path
from catalog import Catalog
from jobs import JobService
with tempfile.TemporaryDirectory() as td:
    p=Path(td);a=p/'a.jsonl';b=p/'b.jsonl';a.write_text(''+json.dumps({'id':'a1','amount':4})+'\n');b.write_text(json.dumps({'id':'b1','amount':9})+'\n')
    c=Catalog();first=c.create('lab','A',a);j=JobService(c);j.submit('old','lab','A');before=j.execute('old')
    c.move(('lab','A'),('prod','A'));second=c.create('lab','A',b);wrong=j.execute('old');j.submit('new','lab','A');new=j.execute('new')
    c.names[('lab','A')]=first;disconnected=j.execute('new')
    print(json.dumps({'normal':before,'common_route_failure':{'old_run_after_name_reuse':wrong,'expected_old_sum':4,'old_dataset':first,'new_dataset':second},'coupling':{'fixed_business_request':'execute new job','new_catalog_binding':new,'stale_catalog_binding':disconnected,'required_new_sum':9},'scope':'真实文件读取受目录解析驱动；起点还没有提交时快照绑定。'},ensure_ascii=False))
