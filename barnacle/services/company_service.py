import jsonpickle
import os

from barnacle.models.company import Company
from barnacle.models.filing import Filing


class CompanyService:
    @classmethod
    def make_from_filepath(cls, filepath):
        if not os.path.exists(filepath):
            raise ArgumentError(f"{filepath} does not exist.")

        content = None
        with open(filepath, "r") as f:
            content = f.read()

        company_dict = jsonpickle.decode(content)
        filings_json = company_dict["filings"]
        filings = []
        for filing_json in filings_json:
            filings.append(Filing(**filing_json))

        co = Company(company_dict["name"], company_dict["sic"], filings)
        return co
