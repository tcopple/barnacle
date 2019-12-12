class Portfolio(object):
    def __init__(self, name=None, cik=None, holdings=None, **kwargs):
        self.cik = cik
        self.name = name
        self.holdings = holdings
        self.holdings_by_cusip = {holding.cusip: holding for holding in holdings}

    def cusips(self):
        return self.holdings_by_cusip.keys()

    def aum(self):
        return sum(holding.value for holding in self.holdings)

    def __len__(self):
        return len(self.holdings)
