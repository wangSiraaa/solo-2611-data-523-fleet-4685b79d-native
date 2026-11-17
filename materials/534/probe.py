import tempfile,json
from source_registry import SourceRegistry
from ingestion import Ingestion
with tempfile.TemporaryDirectory() as td:
    r=SourceRegistry();i=Ingestion(r,td+'/out.db');i.read('A','s1','shard-1.csv');normal=i.db.execute('select sum(value) from imported').fetchone()[0]
    t=r.acquire('A');r.maintain('A');i.read('A','s2','shard-2.csv');wrong=i.calls[-1]
    try:r.finish_maintenance('A')
    except RuntimeError as e:blocked=str(e)
    r.release(t);r.finish_maintenance('A');closed=r.states['A'];i.read('B','s3','shard-2.csv');actual=i.db.execute('select shard,value,source from imported order by shard').fetchall()
    print(json.dumps({'normal':{'first_import_sum':normal},'common_route_failure':{'new_read_after_maintenance':wrong,'expected_new_source':'B'},'coupling':{'close_with_held_read':blocked,'after_real_release_state':closed,'actual_imports':actual,'actual_source_calls':i.calls},'scope':'真实文件读取/SQLite导入和引用消费；资格替代、ticket仲裁、真实底层close和回调幂等仍待实现。'},ensure_ascii=False))

import threading
from controlled_source import ControlledSource
src=ControlledSource('A','shard-1.csv');src.allow_read.clear();received=[];th=threading.Thread(target=lambda:received.append(src.read()));th.start()
assert src.before_read.wait(2)
try:src.close()
except RuntimeError as e:real_close_block=str(e)
src.allow_read.set();th.join(2);assert not th.is_alive();src.close()
print(json.dumps({'real_controlled_source':{'held_read_close':real_close_block,'received_bytes':len(received[0]),'closed':src.closed,'audit':src.audit}},ensure_ascii=False))

src2=ControlledSource('A','shard-2.csv');src2.allow_callback.clear();received2=[];th2=threading.Thread(target=lambda:received2.append(src2.read()));th2.start();assert src2.before_callback.wait(2)
try:src2.close()
except RuntimeError as e:callback_block=str(e)
src2.allow_callback.set();th2.join(2);assert not th2.is_alive();src2.close()
print(json.dumps({'real_callback_hold':{'close_after_actual_file_read_before_return':callback_block,'received_bytes':len(received2[0]),'closed_after_callback':src2.closed,'audit':src2.audit}},ensure_ascii=False))
