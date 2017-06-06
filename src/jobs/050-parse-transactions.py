import os
import csv
import glob
import pandas
import luigi
import jsonpickle
from models.portfolio import Portfolio
from helpers.file_helpers import FileHelpers
from services.portfolio_service import PortfolioService

class FileOutputTask(luigi.ExternalTask):
    filepath = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.filepath)

class GenerateAllTransactions(luigi.WrapperTask):
    REPORTS_PATH = FileHelpers.CONFIG["PATH_COMPANY_REPORTS"]
    TRANSACTIONS_PATH = FileHelpers.CONFIG["PATH_TRANSACTIONS"]

    def requires(self):
        #filing_combinations = [(None, self.input()[0])] + list(zip(self.input()[0:-1], self.input()[1:]))
        pass

class GenerateTransactions(luigi.Task):
    TRANSACTIONS_PATH = FileHelpers.CONFIG["PATH_TRANSACTIONS"]
    lhs_filepath = luigi.Parameter(default="")
    rhs_filepath = luigi.Parameter(default="")

    def requires(self):
        return [FileOutputTask(self.lhs_filepath), FileOutputTask(self.rhs_filepath)]

    def run(self):
        transactions = [["date", "type", "cusip", "size", "name", "asset", "allocation"]]
        lhs_filepath = self.input()[0]
        rhs_filepath = self.input()[1]

        with lhs_filepath.open("r") as lhs_handle:
            lhs_json = lhs_handle.read()

        with rhs_filepath.open("r") as rhs_handle:
            rhs_json = rhs_handle.read()

        holdings_lhs = jsonpickle.decode(lhs_json)
        holdings_rhs = jsonpickle.decode(rhs_json)

        for transaction in PortfolioService.transform(holdings_lhs, holdings_rhs):
            date = os.path.basename(rhs_filepath.fn)[0:8]
            transactions.append([date, transaction.type, transaction.cusip, transaction.size, transaction.name, transaction.asset, transaction.allocation])

        with self.output().open('w') as fh:
            writer = csv.writer(fh)
            writer.writerows(transactions)

    def output(self):
        end_date = os.path.basename(self.rhs_filepath)[0:8]
        filepath = os.path.join(self.TRANSACTIONS_PATH, "{}-transactions.csv".format(end_date))
        return luigi.LocalTarget(filepath)
