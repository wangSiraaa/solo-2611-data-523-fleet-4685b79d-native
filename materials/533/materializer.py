import csv,io
class Materializer:
    def __init__(self,meter):self.meter=meter;self.consumers=[];self.index=0;self.outputs=[]
    def attach(self,consumer):self.consumers.append(consumer)
    def detach(self,consumer):self.consumers.remove(consumer)
    def step(self,raw):
        rows=list(csv.DictReader(io.StringIO(raw.decode())));out=io.StringIO();w=csv.writer(out,lineterminator='\n');w.writerow(['row_id','value'])
        for r in rows:w.writerow([r['row_id'],int(r['value'])*3])
        result=out.getvalue().encode();cost=len(raw)+len(result);self.meter.record(self.index,cost,self.consumers);self.index+=1;self.outputs.append(result);return result
