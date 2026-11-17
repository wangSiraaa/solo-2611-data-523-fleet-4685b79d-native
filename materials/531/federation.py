class Federation:
    def __init__(self,budget,source):self.budget=budget;self.source=source
    def run(self,query_id,minimum,lose_response=False):
        self.budget.reserve(query_id,self.source.upper_bound())
        try:
            ticket=self.source.scan(query_id,lose_response);rows=[r for r in ticket['rows'] if r['amount']>=minimum]
            self.budget.settle(query_id,len(rows));return rows
        except TimeoutError:
            self.budget.settle(query_id,0);return None
