import json
from pathlib import Path
from source import Source
from budget import BudgetLedger
from federation import Federation
s=Source(Path('orders.jsonl'));b=BudgetLedger(1000);f=Federation(b,s);rows=f.run('q1',50);state=b.state();expected=next(iter(s.receipts.values()))['scanned_bytes']
b2=BudgetLedger(1000);f2=Federation(b2,s);f2.run('q2',50,lose_response=True);lost=b2.state()
b3=BudgetLedger(1);f3=Federation(b3,s);before=s.scan_count
try:f3.run('q3',50)
except ValueError:pass
print(json.dumps({'normal':{'rows':rows},'common_route_failure':{'charged':state['spent'],'actual_scanned_bytes':expected,'lost_reply_account':lost,'source_receipt_exists':len(s.receipts)==2},'coupling':{'sufficient_budget_real_scans':before,'insufficient_budget_additional_scans':s.scan_count-before,'rejected_account':b3.state()},'scope':'起点真实文件扫描和预留路径；未知费用义务/真实结算与两源join未实现。'},ensure_ascii=False))

from paged_source import PagedSource
pages=PagedSource('orders',Path('orders.jsonl'),page_rows=1);customer=PagedSource('customers',Path('customers.jsonl'),page_rows=1)
not_started=pages.get_ticket('page-query','0')
try:pages.scan('page-query','0',lose_response=True)
except TimeoutError:pass
unknown=pages.get_ticket('page-query','0');pages.resolve_for_test('page-query','0');done=pages.get_ticket('page-query','0');repeat=pages.scan('page-query','0');customers=customer.scan('customer-query','0')
print(json.dumps({'paged_source_fixture':{'catalog':pages.catalog(),'not_started':not_started,'after_lost_response':unknown,'after_resolution':done,'repeat_same_ticket':repeat['ticket_id']==done['ticket_id'],'actual_scan_count':len(pages.audit),'customer_rows':customers['rows']},'scope':'仅真实分页来源与票据支持，不是目标联查/费用结算实现。'},ensure_ascii=False))
