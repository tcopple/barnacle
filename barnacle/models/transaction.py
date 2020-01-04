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

    def __repr__(self):
        return f"TRANSACTION([type:{self.type}], [size:{self.size}], [cusip:{self.cusip}], [name:{self.name}], [asset:{self.asset}])"
