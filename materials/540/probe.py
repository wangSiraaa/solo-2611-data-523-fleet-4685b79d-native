import json
import tempfile
from pathlib import Path
from replay import Replay
from readers import Readers


def run():
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp)/'replica.sqlite'
        r, q = Replay(p), Readers(p)
        records = [
          {'lsn':1,'kind':'begin','tx':'seed'},
          {'lsn':2,'kind':'put','tx':'seed','key':'a','value':10},
          {'lsn':3,'kind':'put','tx':'seed','key':'b','value':20},
          {'lsn':4,'kind':'commit','tx':'seed'}]
        for x in records:r.feed(x)
        q.begin('normal')
        normal=q.next('normal',10)
        q.close('normal')
        q.begin('old')
        first=q.next('old',1)
        # A has not committed when independent B commits.
        records=[
          {'lsn':5,'kind':'begin','tx':'A'},
          {'lsn':6,'kind':'put','tx':'A','key':'x','value':99},
          {'lsn':7,'kind':'begin','tx':'B'},
          {'lsn':8,'kind':'put','tx':'B','key':'b','value':30},
          {'lsn':9,'kind':'commit','tx':'B'}]
        for x in records:r.feed(x)
        q.begin('leak')
        leaked=q.next('leak',20)
        q.close('leak')
        r.feed({'lsn':10,'kind':'abort','tx':'A'})
        cleanup=r.feed({'lsn':11,'kind':'cleanup','horizon':9},now=100)
        rest=q.next('old',10)
        out={'normal':normal,'first_page':first,'interleaved':leaked,
             'cleanup':cleanup,'old_remaining':rest,'old_status':q.status('old'),
             'inspect':r.inspect()}
        r.shutdown();q.shutdown()
        return out


if __name__=='__main__':
    print(json.dumps(run(),ensure_ascii=False,indent=2))
