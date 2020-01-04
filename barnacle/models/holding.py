from abc import ABCMeta, abstractmethod
from enum import IntEnum, unique


class HoldingFactory(object):
    @classmethod
    def make(cls, attrs):

        issuer = attrs["nameOfIssuer"]
        title_of_class = attrs["titleOfClass"]
        cusip = attrs["cusip"]
        value = float(attrs["value"]) * 1000.0
        shares = None
        principle = None
        option_type = None

        ctor = None
        share_type = attrs["shrsOrPrnAmt"]["sshPrnamtType"]
        if share_type == "SH":
            shares = float(attrs["shrsOrPrnAmt"]["sshPrnamt"])
            return ShareHolding(issuer, title_of_class, cusip, value, shares)

        elif share_type == "PRN":
            principle = float(attrs["shrsOrPrnAmt"]["sshPrnamt"])
            option_type = attrs["putCall"]
            return OptionHolding(
                issuer, title_of_class, cusip, value, principle, option_type
            )

        raise Exception(f"HoldingFactory cannot handle type [{share_type}].")


class Holding(object, metaclass=ABCMeta):
    def __init__(self, issuer, title, cusip, value):
        self.issuer = issuer
        self.title_of_class = title
        self.cusip = cusip
        self.value = value

    def allocation(self, total_calculator):
        return self.value / total_cacluator()

    @abstractmethod
    def price(self):
        raise NotImplementedError()


class ShareHolding(Holding):
    def __init__(self, issuer, title, cusip, value, shares):
        super().__init__(issuer, title, cusip, value)

        self.shares = shares

    def price(self):
        return self.value / self.shares


class OptionHolding(Holding):
    def __init__(self, issuer, title, cusip, value, principle, option_type):
        super().__init__(issuer, title, cusip, value)

        self.size = principle
        self.principle = principle
        self.option_type = option_type

    def price(self):
        return self.value / self.size
