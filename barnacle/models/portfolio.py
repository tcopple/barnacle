class Portfolio(object):
    def __init__(self, name=None, cik=None, holdings=None):
        self.cik = cik
        self.name = name
        self.holdings = holdings

        self.__holdings_by_cusip = {holding.cusip: holding for holding in holdings}

    def cusips(self):
        return self.holdings_by_cusip.keys()

    def aum(self):
        return sum(holding.value for holding in self.holdings)

    def __len__(self):
        return len(self.holdings)

    def __str__(self):
        return (
            f"{self.cik}-{self.name}: [{self.aum()}] assets in [{len(self)}] holdings."
        )

    def __repr__(self):
        return str(self)
