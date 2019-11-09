class Holding(object):
    def __init__(self, **kwargs):
        self.issuer = kwargs["nameOfIssuer"]
        self.title_of_class = kwargs["titleOfClass"]
        self.cusip = kwargs["cusip"]
        self.value = float(kwargs["value"]) * 1000
        self.shares = (
            float(kwargs["shrsOrPrnAmt"]["sshPrnamt"])
            if kwargs["shrsOrPrnAmt"]["sshPrnamtType"] == "SH"
            else None
        )
        self.principle = (
            float(kwargs["shrsOrPrnAmt"]["sshPrnamt"])
            if kwargs["shrsOrPrnAmt"]["sshPrnamtType"] == "PRN"
            else None
        )
        self.option_type = kwargs["putCall"] if "putCall" in kwargs else None

    def allocation(self, ttl_functor):
        return self.value / ttl_functor()

    def price(self):
        return self.value / float(self.shares if self.shares else self.principle)

    def size(self):
        return self.shares if self.shares else self.principle
