import csv,io,hashlib
from pathlib import Path
def describe(path):
    raw=Path(path).read_bytes();out=io.StringIO();w=csv.writer(out,lineterminator='\n');w.writerow(['row_id','value'])
    for row in csv.DictReader(io.StringIO(raw.decode())):w.writerow([row['row_id'],int(row['value'])*3])
    return {'sha256':hashlib.sha256(raw).hexdigest(),'input_bytes':len(raw),'output_max_bytes':len(out.getvalue().encode()),'transform':'times-three-v1'}
