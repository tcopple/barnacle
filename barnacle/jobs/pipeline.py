import csv
import glob
import itertools
import json
import urllib.request
from os.path import basename, dirname, join

import jsonpickle
import luigi
import pandas
from gluish import BaseTask
from luigi.contrib.s3 import S3Client, S3PathTask, S3Target

from barnacle.config import BarnacleConfig
from barnacle.helpers.file import FileHelpers
from barnacle.jobs.core import FileOutputTask, S3OutputTask
from barnacle.models.portfolio import Portfolio
from barnacle.services.portfolio import PortfolioService

s3_client = (
    S3Client(BarnacleConfig.S3_AWS_ACCESS_KEY, BarnacleConfig.S3_AWS_SECRET_KEY)
    if hasattr(BarnacleConfig, "S3_BUCKET")
    else None
)


def DelegatingTarget(path, *args, **kwargs):
    if path.startswith("s3://"):
        return S3Target(path, *args, client=s3_client, **kwargs)

    return luigi.local_target.LocalTarget(path, *args, **kwargs)


def DelegatingFilePath(path, *args, **kwargs):
    if path.startswith("s3://"):
        file_list = s3_client.list(path)
        return [S3OutputTask(join(path, fp), s3_client) for fp in file_list]

    return [FileOutputTask(fp) for fp in glob.glob(join(path, "*"))]


def DelegatingFile(filepath):
    if filepath.startswith("s3://"):
        return S3OutputTask(filepath, s3_client)

    return FileOutputTask(filepath)


### 010
class FetchAllSECIndexes(luigi.WrapperTask):
    start_year = luigi.Parameter(default=BarnacleConfig.START_YEAR)
    end_year = luigi.Parameter(default=BarnacleConfig.END_YEAR)

    def requires(self):
        for year in range(int(self.start_year), int(self.end_year)):
            yield FetchSECIndexForYear(year)


class FetchSECIndexForYear(luigi.WrapperTask):
    year = luigi.Parameter()

    def requires(self):
        for qtr in ["QTR1", "QTR2", "QTR3", "QTR4"]:
            yield FetchSECIndex(self.year, qtr)


class FetchSECIndex(luigi.Task):
    URL_BASE = BarnacleConfig.SEC_URL_BASE
    FILINGS_PATH = BarnacleConfig.PATH_FILINGS

    year = luigi.Parameter()
    quarter = luigi.Parameter()

    def requires(self):
        pass

    def run(self):
        remote_path = f"{self.URL_BASE}/{self.year}/{self.quarter}/form.idx"
        output = urllib.request.urlopen(remote_path).read().decode("utf-8")
        with self.output().open("w") as handle:
            print(output, file=handle)

    def output(self):
        path = join(self.FILINGS_PATH, f"{self.year}{self.quarter}.idx")
        return DelegatingTarget(path)


### 020
class ParseAllSECFilings(luigi.WrapperTask):
    raw_path = luigi.Parameter(default=BarnacleConfig.PATH_FILINGS)

    def requires(self):
        # if lookup path is s3
        if self.raw_path.startswith("s3://"):
            files = s3_client.listdir(self.raw_path)
            for fh in files:
                yield ParseSECFiling(fh)

        # if lookup path is local disk
        else:
            files = glob.glob(join(self.raw_path, "*"))
            for fh in files:
                yield ParseSECFiling(fh)


class ParseSECFiling(luigi.Task):
    CSVS_PATH = BarnacleConfig.PATH_FILINGS_CSVS
    filepath = luigi.Parameter()

    def run(self):
        csv_lines = [["form", "company", "sic", "date", "path"]]

        # TODO handle switch between s3 and local types better
        with DelegatingTarget(self.filepath).open() as fh:
            lines_buffer = fh.readlines()

            # drop first 10 lines cause they're a constant header
            del lines_buffer[:10]

            # parse fields as fixed width fields
            fieldwidths = (12, 62, 12, 12, 43)
            for line in lines_buffer:
                parser = self.make_parser(fieldwidths)
                fields = [field.strip(" ") for field in parser(line)]
                csv_lines.append(fields)

        with self.output().open("w") as handle:
            writer = csv.writer(handle)
            writer.writerows(csv_lines)

    def output(self):
        output_name = FileHelpers.remove_extension(basename(self.filepath))
        return DelegatingTarget(join(self.CSVS_PATH, f"{output_name}.csv"))

    def make_parser(self, fieldwidths):
        cuts = tuple(cut for cut in itertools.accumulate(abs(fw) for fw in fieldwidths))
        pads = tuple(fw < 0 for fw in fieldwidths)  # bool values for padding fields
        flds = tuple(itertools.zip_longest(pads, (0,) + cuts, cuts))[:-1]
        parser = lambda line: tuple(line[i:j] for pad, i, j in flds if not pad)

        # optional informational function attributes
        parser.size = sum(abs(fw) for fw in fieldwidths)
        parser.fmtstring = " ".join(
            "{}{}".format(abs(fw), "x" if fw < 0 else "s") for fw in fieldwidths
        )

        return parser


### 030
class DownloadFiling(luigi.ExternalTask):
    output_path = luigi.Parameter()
    filing_path = luigi.Parameter()

    def requires(self):
        pass

    def run(self):
        output = urllib.request.urlopen(self.filing_path).read().decode("utf-8")
        with self.output().open("w") as handle:
            print(output, file=handle)

    def output(self):
        return DelegatingTarget(self.output_path)


class DownloadFilings(luigi.Task):
    CSVS_PATH = luigi.Parameter(default=BarnacleConfig.PATH_FILINGS_CSVS)
    REPORTS_PATH = BarnacleConfig.PATH_COMPANY_REPORTS

    sic = luigi.Parameter(default="")
    filing_type = luigi.Parameter(default="")
    company_name = luigi.Parameter(default="")

    def requires(self):
        return DelegatingFilePath(self.CSVS_PATH)

    def run(self):
        frames = []
        for fh in self.input():
            with fh.open("r") as in_file:
                frame = pandas.read_csv(in_file, index_col=None)

                if self.filing_type:
                    frame = frame[frame.form.str.contains(self.filing_type)]

                if self.company_name:
                    frame = frame[frame.company == self.company_name]

                if self.sic:
                    frame = frame[frame.sic == int(self.sic)]

                frames.append(frame)

        combined = pandas.concat(frames)
        for index, row in combined.iterrows():
            filename = "{}-{}".format(
                row["date"].replace("-", ""), row["form"].replace("/", "")
            )
            company_path = (
                str(int(row["sic"]))
                + "-"
                + row["company"][0:10]
                .lower()
                .replace(" ", "")
                .replace("/", "")
                .replace("&", "")
                .replace("(", "")
                .replace(")", "")
                .replace(".", "")
                .replace(",", "")
            )
            local_path = join(self.REPORTS_PATH, company_path, filename)
            remote_path = "https://www.sec.gov/Archives/{}".format(row["path"])

            yield DownloadFiling(local_path, remote_path)

    def output(self):
        pass


# 040
class GenerateHoldings(luigi.WrapperTask):
    REPORTS_PATH = BarnacleConfig.PATH_COMPANY_REPORTS
    TRANSACTIONS_PATH = BarnacleConfig.PATH_COMPANY_HOLDINGS

    file_mask = luigi.Parameter(default="")

    def requires(self):
        output_tasks = DelegatingFilePath(self.file_mask)
        for task in output_tasks:
            yield GenerateHolding(task.s3_filepath)


class GenerateHolding(luigi.Task):
    REPORTS_PATH = BarnacleConfig.PATH_COMPANY_REPORTS
    TRANSACTIONS_PATH = BarnacleConfig.PATH_COMPANY_HOLDINGS

    input_file = luigi.Parameter(default="")

    def requires(self):
        # TODO should be file or s3
        print(self.input_file)
        return S3OutputTask(self.input_file)

    def run(self):
        transactions = [
            ["date", "type", "cusip", "size", "name", "asset", "allocation"]
        ]

        with self.input().open("r") as in_file:
            holdings = in_file.read()

        portfolio_lhs = Portfolio(holdings=[])
        if holdings and PortfolioService.is_xml_representation(holdings):
            portfolio_holdings = PortfolioService.make_from_xml(holdings)
        elif holdings and PortfolioService.is_tabular_representation(holdings):
            portfolio_holdings = PortfolioService.make_from_table(holdings)

        if portfolio_holdings is None or len(portfolio_holdings) == 0:
            raise Exception("No holdings available.")

        data = jsonpickle.encode(portfolio_holdings)
        with self.output().open("w") as fh:
            fh.write(json.dumps(json.loads(data), indent=4))

    def output(self):
        date_prefix = basename(self.input_file)[0:8]
        path_prefix = dirname(self.input_file)

        output_filename = f"{date_prefix}.holdings.json"

        output_filepath = path_prefix.replace(
            dirname(path_prefix), self.TRANSACTIONS_PATH
        )

        transaction_filepath = join(output_filepath, output_filename)
        return DelegatingTarget(transaction_filepath)


# 050
class GenerateAllTransactions(luigi.WrapperTask):
    REPORTS_PATH = BarnacleConfig.PATH_COMPANY_REPORTS
    TRANSACTIONS_PATH = BarnacleConfig.PATH_TRANSACTIONS

    file_path = luigi.Parameter(default="")

    def requires(self):
        file_list = [
            join(self.file_path, fp) for fp in list(s3_client.list(self.file_path))
        ]
        filing_combinations = [(None, file_list[0])] + list(
            zip(file_list[0:-1], file_list[1:])
        )

        for fc in filing_combinations:
            yield GenerateTransactions(fc[0], fc[1])


class GenerateTransactions(luigi.Task):
    TRANSACTIONS_PATH = BarnacleConfig.PATH_TRANSACTIONS
    lhs = luigi.Parameter(default="")
    rhs = luigi.Parameter(default="")

    def requires(self):
        return [DelegatingFile(self.lhs), DelegatingFile(self.rhs)]

    def run(self):
        transactions = [
            ["date", "type", "cusip", "size", "name", "asset", "allocation"]
        ]

        lhs_filepath = self.input()[0]
        rhs_filepath = self.input()[1]

        with lhs_filepath.open("r") as lhs_handle:
            lhs_json = lhs_handle.read()

        with rhs_filepath.open("r") as rhs_handle:
            rhs_json = rhs_handle.read()

        holdings_lhs = jsonpickle.decode(lhs_json)
        holdings_rhs = jsonpickle.decode(rhs_json)

        for transaction in PortfolioService.transform(holdings_lhs, holdings_rhs):
            date = basename(rhs_filepath.path)[0:8]
            transactions.append(
                [
                    date,
                    transaction.type,
                    transaction.cusip,
                    transaction.size,
                    transaction.name,
                    transaction.asset,
                    transaction.allocation,
                ]
            )

        with self.output().open("w") as fh:
            writer = csv.writer(fh)
            writer.writerows(transactions)

    def output(self):
        end_date = basename(self.rhs)[0:8]
        dir_name = basename(dirname(self.rhs))
        filepath = join(
            self.TRANSACTIONS_PATH, dir_name, f"{end_date}-transactions.csv"
        )

        return DelegatingTarget(filepath)
