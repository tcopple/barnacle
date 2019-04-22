class Holding(object):
    def __init__(self, attrs):
        self.issuer = attrs["nameOfIssuer"]
        self.title_of_class = attrs["titleOfClass"]
        self.cusip = attrs["cusip"]
        self.value = float(attrs["value"]) * 1000
        self.shares = (
            float(attrs["shrsOrPrnAmt"]["sshPrnamt"])
            if attrs["shrsOrPrnAmt"]["sshPrnamtType"] == "SH"
            else None
        )
        self.principle = (
            float(attrs["shrsOrPrnAmt"]["sshPrnamt"])
            if attrs["shrsOrPrnAmt"]["sshPrnamtType"] == "PRN"
            else None
        )
        self.option_type = attrs["putCall"] if "putCall" in attrs else None

    def allocation(self, ttl_functor):
        return self.value / ttl_functor()

    def price(self):
        return self.value / float(self.shares if self.shares else self.principle)

    def size(self):
        return self.shares if self.shares else self.principle
