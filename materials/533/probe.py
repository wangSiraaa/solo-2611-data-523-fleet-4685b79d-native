import json
from pathlib import Path
from fractions import Fraction
from meter import Meter
from materializer import Materializer
m=Meter();w=Materializer(m);w.attach('A');w.attach('B');out=w.step(Path('batch.csv').read_bytes());cost=m.segments[0]['cost'];normal=m.bill(w.consumers);w.detach('A');wrong=m.bill(w.consumers);w.attach('C');after=m.bill(w.consumers);w.step(Path('batch.csv').read_bytes())
print(json.dumps({'normal':{'result_csv':out.decode(),'actual_cost':cost,'two_participants_bill':normal},'common_route_failure':{'after_A_left':wrong,'expected_original_segment':{'A':str(Fraction(cost,2)),'B':str(Fraction(cost,2))},'late_C_retroactive_bill':after},'coupling':{'segment0_participants':m.segments[0]['participants'],'segment1_participants':m.segments[1]['participants'],'physical_segments':len(m.segments)},'scope':'真实CSV字节计量与参与者快照；预算、权益、接入transfer和固定分段结算未实现。'},ensure_ascii=False))

from catalog import describe
print(json.dumps({'source_catalog':describe('batch.csv')},ensure_ascii=False))
