class BudgetLedger:
    def __init__(self,credits):self.available=credits;self.reserved={};self.spent=0
    def reserve(self,key,n):
        if n>self.available:raise ValueError('BUDGET')
        self.available-=n;self.reserved[key]=n
    def settle(self,key,used):
        n=self.reserved.pop(key);self.spent+=used;self.available+=n-used
    def state(self):return {'available':self.available,'reserved':sum(self.reserved.values()),'spent':self.spent}
