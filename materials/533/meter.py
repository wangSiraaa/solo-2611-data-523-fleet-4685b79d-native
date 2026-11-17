from fractions import Fraction
class Meter:
    def __init__(self):self.segments=[];self.final_consumers=[]
    def record(self,segment,cost,participants):self.segments.append({'segment':segment,'cost':cost,'participants':list(participants)})
    def bill(self,current):
        total=sum(s['cost'] for s in self.segments);return {x:str(Fraction(total,len(current))) for x in current} if current else {}
