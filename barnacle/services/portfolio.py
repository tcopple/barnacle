import re

import numpy
import xmltodict

from barnacle.helpers.etc import xml_to_dict
from barnacle.helpers.file import FileHelpers
from barnacle.models.holding import Holding
from barnacle.models.portfolio import Portfolio
from barnacle.models.transaction import Transaction


class PortfolioService:
    @staticmethod
    def is_xml_representation(xml):
        match = re.findall("<xml>(.*?)</xml>", xml, re.DOTALL | re.IGNORECASE)
        return False if not match else True

    @staticmethod
    def is_tabular_representation(table):
        match = re.findall("<table>(.*?)</table>", table, re.DOTALL | re.IGNORECASE)
        return False if not match else True

    @staticmethod
    def make_from_xml(xml):
        matches = re.findall("<xml>(.*?)</xml>", xml, re.DOTALL | re.IGNORECASE)
        if not matches:
            return None

        if len(matches) < 2:
            return None

        deets = xml_to_dict(matches[0].strip())
        fi = deets.get("edgarSubmission", {}).get("headerData", {}).get("filerInfo", {})
        fi.get("periodOfReport", None)
        cik = fi.get("filer", {}).get("credentials", {}).get("cik", None)
        name = (
            deets.get("edgarSubmission", {})
            .get("formData", {})
            .get("coverPage", {})
            .get("filingManager", {})
            .get("name", None)
        )
        portfolio = xml_to_dict(matches[1].strip())
        holdings_json = portfolio["informationTable"]["infoTable"]
        holdings = [Holding(**holding) for holding in holdings_json]
        return Portfolio(name=name, cik=cik, holdings=holdings)

    @staticmethod
    def make_from_table(table):
        raise Exception("Not implemented.")

        # this is a rough implementation it needs to be tested across a variety of the tabular file formats
        match = re.findall(
            "(?:<TABLE>\s+)(?:<C>\s+)*(.*)</TABLE>", table, re.DOTALL | re.IGNORECASE
        )

        if not match:
            return None

        holdings = []
        #  fieldwidths = (32, 18, 14, 14, 8, 10, 9, 8, 9)
        fieldwidths = (31, 17, 9, 9, 9, 3, 7, 20, 9, 8, 8)
        lines = [s for s in match[0].splitlines()]
        lines.pop(0)
        lines.pop(0)
        lines.pop(0)
        lines.pop(0)

        for line in lines:
            parser = FileHelpers.make_fixed_width_parser(fieldwidths)
            fields = [field.strip(" ") for field in parser(line)]
            #  fields = [field for field in parser(line)]
            print(fields)

        return Portfolio(holdings)

    @staticmethod
    def transform(from_portfolio, to_portfolio):
        transactions = []

        new_positions = set(to_portfolio.cusips()) - set(from_portfolio.cusips())
        for cusip in new_positions:
            holding = to_portfolio.holdings_by_cusip[cusip]
            transactions.append(
                Transaction(
                    {
                        "cusip": cusip,
                        "size": holding.size(),
                        "name": holding.issuer,
                        "asset": holding.title_of_class,
                        "allocation": holding.allocation(to_portfolio.aum),
                        "type": "OPEN",
                    }
                )
            )

        closed_positions = set(from_portfolio.cusips()) - set(to_portfolio.cusips())
        for cusip in closed_positions:
            holding = from_portfolio.holdings_by_cusip[cusip]
            transactions.append(
                Transaction(
                    {
                        "cusip": cusip,
                        "size": -holding.size(),
                        "name": holding.issuer,
                        "asset": holding.title_of_class,
                        "allocation": -holding.allocation(to_portfolio.aum),
                        "type": "CLOSE",
                    }
                )
            )

        changed_positions = [
            cusip for cusip in to_portfolio.cusips() if cusip in from_portfolio.cusips()
        ]
        for cusip in changed_positions:
            from_h = from_portfolio.holdings_by_cusip[cusip]
            to_h = to_portfolio.holdings_by_cusip[cusip]

            attrs = {
                "cusip": from_h.cusip,
                "size": to_h.size() - from_h.size(),
                "name": from_h.issuer,
                "asset": from_h.title_of_class,
                "allocation": to_h.allocation(to_portfolio.aum)
                - from_h.allocation(from_portfolio.aum),
                "type": "UPDATE",
            }

            transactions.append(Transaction(attrs))

        return transactions
