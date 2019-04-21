import os
import csv
import glob
import pandas
import luigi
import json
import jsonpickle
from models.portfolio import Portfolio
from helpers.file_helpers import FileHelpers
from services.portfolio_service import PortfolioService

class FileOutputTask(luigi.ExternalTask):
    filepath = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.filepath)

class GenerateHoldings(luigi.WrapperTask):
    REPORTS_PATH = FileHelpers.CONFIG["PATH_COMPANY_REPORTS"]
    TRANSACTIONS_PATH = FileHelpers.CONFIG["PATH_COMPANY_HOLDINGS"]
    file_mask = luigi.Parameter(default="")

    def requires(self):
        files = glob.glob(self.file_mask)
        print(files)
        for filepath in files:
            yield GenerateHolding(filepath)

class GenerateHolding(luigi.Task):
    REPORTS_PATH = FileHelpers.CONFIG["PATH_COMPANY_REPORTS"]
    TRANSACTIONS_PATH = FileHelpers.CONFIG["PATH_COMPANY_HOLDINGS"]

    input_file = luigi.Parameter(default="")

    def requires(self):
        return FileOutputTask(self.input_file)

    def run(self):
        transactions = [["date", "type", "cusip", "size", "name", "asset", "allocation"]]
        with self.input().open('r') as in_file:
            holdings = in_file.read()

        portfolio_lhs = Portfolio([])
        if holdings and PortfolioService.is_xml_representation(holdings):
            portfolio_holdings = PortfolioService.make_from_xml(holdings)
        elif holdings and PortfolioService.is_tabular_representation(holdings):
            portfolio_holdings = PortfolioService.make_from_table(holdings)

        data = jsonpickle.encode(portfolio_holdings)
        with self.output().open('w') as fh:
            fh.write(json.dumps(json.loads(data), indent=4))

    def output(self):
        output_date = os.path.basename(self.input_file)[0:8]
        output_filename = "{}.holdings.json".format(output_date)
        output_filepath = os.path.dirname(self.input_file).replace(self.REPORTS_PATH, self.TRANSACTIONS_PATH)

        transaction_filepath = os.path.join(output_filepath, output_filename)
        return luigi.LocalTarget(transaction_filepath)
