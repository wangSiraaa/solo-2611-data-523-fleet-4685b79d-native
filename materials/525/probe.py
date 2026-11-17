import tempfile,json
from pathlib import Path
from runs import RunDirectory
from intervals import days,upstream
with tempfile.TemporaryDirectory() as td:
    p=Path(td);d=RunDirectory(p/'runs.sqlite');req=[('extract','2026-01-01','2026-01-02')]
    before=d.ready(req)
    r=d.submit(*req[0]);d.finish(r,{'sum':4});after=d.ready(req)
    disconnected=d.ready([('extract','2026-01-02','2026-01-03')])
    duplicate=d.submit(*req[0])
    result={'normal_day_intervals':days('2026-01-01','2026-01-03'),'coupling':{'business_input':req,'before':before,'after':after,'disconnected_wrong_requirement':disconnected,'expected_after':True},'common_route_failure':{'first_run':r,'second_run':duplicate,'same_logical_interval_count':d.db.execute('select count(*) from runs').fetchone()[0],'expected_count':1},'weekly_current_requirements':upstream('weekly','2026-01-05','2026-01-12'),'scope':'起点正常日路径和真实状态消费；不等于已实现回补目标。'}
    print(json.dumps(result,ensure_ascii=False))
