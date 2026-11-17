import tempfile,json
from pathlib import Path
from facts import Facts
from settlement import Settlement
with tempfile.TemporaryDirectory() as td:
    f=Facts(Path(td)/'facts.db');s=Settlement(Path(td)/'bills.db',f)
    f.append('base','m1','2026-01-01','2026-01-02',100);base=s.seal('2026-01','m1');first=s.reconcile('2026-01','m1')
    f.append('correct','m1','2026-01-01','2026-01-02',120);a=s.reconcile('2026-01','m1');b=s.reconcile('2026-01','m1')
    f.append('revert','m1','2026-01-01','2026-01-02',100);stale=s.reconcile('2026-01','m1')
    print(json.dumps({'normal':{'sealed':base,'same_knowledge_delta':first},'common_route_failure':{'correction_first_delta':a,'same_correction_second_delta':b,'expected_second':0,'posted_adjustments':s.posted('2026-01','m1')},'coupling':{'new_fact_amount':120,'observed_delta':a,'reverted_fact_amount':100,'observed_delta':stale},'scope':'起点真实读取事实并写账单；未实现双时态和累计结算，撤回probe仅为消费变化。'},ensure_ascii=False))
