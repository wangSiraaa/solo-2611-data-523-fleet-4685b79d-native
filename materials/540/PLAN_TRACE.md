# Sample plan trace (`plan.json`)

One working database under `materials/540/work.sqlite`, reopened mid-run.
Each row is the real return value and the resulting `applied_lsn`/`visible_commit`/pending.

```
 0 feed             applied=1  visible=0  pending=null                             {"status": "APPLIED", "lsn": 1, "tx": "seed", "kind": "begin"}
 1 feed             applied=2  visible=0  pending=null                             {"status": "APPLIED", "lsn": 2, "kind": "put", "tx": "seed", "key": "b", "value": 20}
 2 feed             applied=3  visible=0  pending=null                             {"status": "APPLIED", "lsn": 3, "kind": "put", "tx": "seed", "key": "a", "value": 10}
 3 feed             applied=4  visible=4  pending=null                             {"status": "APPLIED", "lsn": 4, "kind": "commit", "tx": "seed", "visible_commit": 4}
 4 begin            applied=4  visible=4  pending=null                             {"query_id": "report", "snapshot": 4, "state": "RUNNING"}
 5 next             applied=4  visible=4  pending=null                             [{"key": "a", "value": 10}]
 6 feed             applied=5  visible=4  pending=null                             {"status": "APPLIED", "lsn": 5, "tx": "hot", "kind": "begin"}
 7 feed             applied=6  visible=4  pending=null                             {"status": "APPLIED", "lsn": 6, "kind": "put", "tx": "hot", "key": "b", "value": 99}
 8 feed             applied=7  visible=7  pending=null                             {"status": "APPLIED", "lsn": 7, "kind": "commit", "tx": "hot", "visible_commit": 7}
 9 feed             applied=7  visible=7  pending={"lsn":8,"horizon":7,"deadline":105} {"status": "WAITING", "lsn": 8, "kind": "cleanup", "horizon": 7, "deadline": 105, "conflicts": [{"id": "report", "snapshot": 4}]}
10 inspect          applied=7  visible=7  pending={"lsn":8,"horizon":7,"deadline":105} {"applied_lsn": 7, "visible_commit": 7, "last_horizon": 0, "versions": [{"key": "a", "born": 4, "dead": null, "value": 10, "deleted": 0},...
11 reopen           applied=7  visible=7  pending={"lsn":8,"horizon":7,"deadline":105} {"reopened": true, "path": "work.sqlite", "grace": 5}
12 next             applied=7  visible=7  pending={"lsn":8,"horizon":7,"deadline":105} [{"key": "b", "value": 20}]
13 next             applied=7  visible=7  pending={"lsn":8,"horizon":7,"deadline":105} []
14 feed             applied=7  visible=7  pending={"lsn":8,"horizon":7,"deadline":105} {"status": "WAITING", "lsn": 8, "kind": "cleanup", "horizon": 7, "deadline": 105, "conflicts": [{"id": "report", "snapshot": 4}]}
15 feed             applied=8  visible=7  pending=null                             {"status": "APPLIED", "lsn": 8, "kind": "cleanup", "horizon": 7, "deadline": 105, "cancelled": ["report"]}
16 next             applied=8  visible=7  pending=null                             ERROR ConflictCancelled cancel_lsn=8
17 close            applied=8  visible=7  pending=null                             {"id": "report", "snapshot": 4, "last_key": "b", "state": "CANCELLED", "cancel_lsn": 8, "cancel_reason": "DEADLINE"}
18 begin            applied=8  visible=7  pending=null                             {"query_id": "fresh", "snapshot": 7, "state": "RUNNING"}
19 next             applied=8  visible=7  pending=null                             [{"key": "a", "value": 10}, {"key": "b", "value": 99}]
20 inspect          applied=8  visible=7  pending=null                             {"applied_lsn": 8, "visible_commit": 7, "last_horizon": 7, "versions": [{"key": "a", "born": 4, "dead": null, "value": 10, "deleted": 0},...
```
