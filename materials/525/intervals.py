from datetime import date, timedelta

def days(start,end):
    a,b=date.fromisoformat(start),date.fromisoformat(end)
    result=[]
    while a<b:
        result.append((a.isoformat(),(a+timedelta(days=1)).isoformat()));a+=timedelta(days=1)
    return result

def upstream(model,start,end):
    return [] if model=='extract' else [('extract',start,end)]
