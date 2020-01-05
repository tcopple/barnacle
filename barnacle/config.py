import os
from os.path import abspath, dirname, join, realpath

try:
    from dotenv import load_dotenv
    load_dotenv(verbose=True)
except ImportError, Exception as e:
    pass

class BarnacleConfig:

    ROOT_PATH = abspath(join(dirname(realpath(__file__)), ".."))

    DIR_DATA = os.environ.get("BARNACLE_DATA") or join(ROOT_PATH, "data")
    DIR_SRC = os.environ.get("BARNACLE_SRC") or join(ROOT_PATH, "src")

    S3_BUCKET = os.environ.get("S3_BUCKET")
    S3_AWS_ACCESS_KEY = os.environ.get("S3_AWS_ACCESS_KEY")
    S3_AWS_SECRET_KEY = os.environ.get("S3_AWS_SECRET_KEY")

    PATH_FILINGS = join(S3_BUCKET, "010-filings")
    PATH_FILINGS_CSVS = join(S3_BUCKET, "020-csvs")
    PATH_COMPANY_REPORTS = join(S3_BUCKET, "030-filings")
    PATH_COMPANY_HOLDINGS = join(S3_BUCKET, "040-holdings")
    PATH_TRANSACTIONS = join(S3_BUCKET, "050-transactions")

    PATH_FIGI_DETAILS = "000-figi"
    PATH_FIGI_MISSING = "000-figi-missing"

    START_YEAR = os.environ.get("BARNACLE_STARTYR") or 2014
    END_YEAR = os.environ.get("BARNACLE_ENDYR") or 2016

    OPENFIGI_URL = "https://api.openfigi.com/v1/mapping"
    SEC_URL_BASE = "https://www.sec.gov/Archives/edgar/full-index"
