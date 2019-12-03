class Transaction(object):
    def __init__(self, attrs):
        self.type = attrs["type"]
        self.cusip = attrs["cusip"]
        self.size = attrs["size"]
        self.name = attrs["name"]
        self.asset = attrs["asset"]
        self.allocation = attrs["allocation"]

    def __str__(self):
        return f"{self.type},{self.size},{self.cusip},{self.name},{self.asset}"
