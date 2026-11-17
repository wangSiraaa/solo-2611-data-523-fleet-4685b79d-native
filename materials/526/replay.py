import sqlite3,json
from audit import schema
class ReplaySession:
    def __init__(self,path):self.db=sqlite3.connect(path);schema(self.db)
    def run(self,events):
        results=[]
        for event in events:
            cur=self.db.execute(event['sql'],event['params']);results.append({'lastrowid':cur.lastrowid,'rowcount':cur.rowcount})
        self.db.commit();return results
