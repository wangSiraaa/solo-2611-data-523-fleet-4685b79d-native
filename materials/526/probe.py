import tempfile,json,sqlite3
from pathlib import Path
from audit import AuditSession,schema
from replay import ReplaySession
with tempfile.TemporaryDirectory() as td:
    p=Path(td);s=sqlite3.connect(p/'source.db');schema(s);a=AuditSession(s)
    c=a.execute('INSERT INTO customers(alias,password) VALUES (?,?)',('PRIVATE_A17','SECRET_P9'));old=c.lastrowid
    a.execute('INSERT INTO orders(id,customer_id,amount) VALUES (?,?,?)',(7,old,5));s.commit();a.export(p/'events.json')
    t=ReplaySession(p/'target.db');t.db.execute('insert into customers(alias,password) values (?,?)',('unrelated','fixture'));t.db.commit()
    t.run(a.events)
    actual=t.db.execute('select c.alias from orders o join customers c on c.id=o.customer_id').fetchone()[0]
    raw=(p/'events.json').read_bytes()
    normal=ReplaySession(p/'normal.db');normal.run(a.events)
    normal_alias=normal.db.execute('select c.alias from orders o join customers c on c.id=o.customer_id').fetchone()[0]
    events2=json.loads(json.dumps(a.events));events2[1]['params'][2]=8
    t2=ReplaySession(p/'changed.db');t2.run(events2)
    changed=t2.db.execute('select amount from orders').fetchone()[0]
    print(json.dumps({'normal_replay_alias':normal_alias,'common_route_failure':{'source_generated_key':old,'target_order_customer_alias':actual,'expected_alias':'PRIVATE_A17','secret_persisted':b'SECRET_P9' in raw},'coupling':{'business_input':'same replay commands apart from recorded public amount','before_amount':5,'after_amount':changed,'disconnected_original_amount':5,'required_after_amount':8},'scope':'起点真实SQL重放及原值泄漏观察；尚未具备安全符号与事务回放。'},ensure_ascii=False))

import sqlite3
from symbol_fixture import resolve_capture
z=sqlite3.connect(':memory:');schema(z);z.execute('insert into customers(alias,password) values (?,?)',('unrelated','x'))
a=z.execute('insert into customers(alias,password) values (?,?)',('synthetic-A','x')).lastrowid
b=z.execute('insert into customers(alias,password) values (?,?)',('synthetic-B','x')).lastrowid
ref={'capture':{'session':'s1','operation':'customer-A','field':'lastrowid'}};key=('s1','customer-A','lastrowid');bindings={key:a}
resolved=resolve_capture(ref,bindings);z.execute('insert into orders(id,customer_id,amount) values (?,?,?)',(1,resolved,7));fresh=z.execute('select alias from customers join orders on customers.id=orders.customer_id where orders.id=1').fetchone()[0]
bindings[key]=b;z.execute('insert into orders(id,customer_id,amount) values (?,?,?)',(2,resolve_capture(ref,bindings),7));changed=z.execute('select alias from customers join orders on customers.id=orders.customer_id where orders.id=2').fetchone()[0]
z.execute('insert into orders(id,customer_id,amount) values (?,?,?)',(3,resolved,7));frozen=z.execute('select alias from customers join orders on customers.id=orders.customer_id where orders.id=3').fetchone()[0]
print(json.dumps({'narrow_symbol_fixture':{'fixed_business_request':{'order_amount':7,'capture':ref},'source_literal_key_would_be':1,'replay_generated_key':a,'correct_capture_binding_alias':fresh,'changed_capture_binding_alias':changed,'frozen_old_capture_alias':frozen},'scope':'仅捕获引用到本轮SQLite生成键的窄消费设施；完整安全录制/保存点符号失效/独立run环境仍待实现。'},ensure_ascii=False))
