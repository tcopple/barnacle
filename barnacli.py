import click
import json
import os
import logging.config

from barnacle.config import BarnacleConfig

CLI_DIR = os.path.dirname(os.path.realpath(__file__))
LOGGING_CONFIG = os.path.join(CLI_DIR, "logging.json")
if os.path.exists(LOGGING_CONFIG):
    with open(LOGGING_CONFIG, "rt") as fh:
        config = json.loads(fh.read())
        logging.config.dictConfig(config)


LOG = logging.getLogger(__name__)
@click.group()
def cli():
    pass


@cli.command()
def config():
    d = {k: v for k, v in dict(vars(BarnacleConfig)).items() if "__" not in k}
    click.echo(json.dumps(d, indent=True, sort_keys=True))

@cli.command()
def find_sic():
    import csv
    import string
    import urllib.request

    from bs4 import BeautifulSoup


    data = [["company", "cik", "sic"]]

    suffixes = string.ascii_lowercase
    suffixes = set(string.ascii_lowercase) - set(["u", "v", "w", "x", "y", "z"])
    suffixes = suffixes | set(["uv", "wxyz", "123"])

    for suffix in sorted(suffixes):
        url = (
            "https://www.sec.gov/divisions/corpfin/organization/cfia-" + suffix + ".htm"
        )
        print("Processing [", url, "].")
        with urllib.request.urlopen(url) as html:
            soup = BeautifulSoup(html.read())
            table = soup.find("table", id="cos")
            rows = table.findAll("tr")
            for row in rows:
                cells = row.find_all("td")
                rows = [ele.text.strip() for ele in cells]
                data.append([ele for ele in rows if ele])

    data = [x for x in data if x]
    with open("sics.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerows(data)

@cli.command()
def get_cusips():
    import jsonpickle
    import pandas
    import requests

    from barnacle.helpers.file import FileHelpers
    from barnacle.services.openfigi import OpenFigiService

    transactions_path = FileHelpers.CONFIG["PATH_TRANSACTIONS"]
    figis_path = FileHelpers.CONFIG["PATH_FIGI_DETAILS"]
    figis_missing_path = FileHelpers.CONFIG["PATH_FIGI_MISSING"]
    FileHelpers.try_make_directory(figis_path)
    FileHelpers.try_make_directory(figis_missing_path)
    figis = [os.path.splitext(f)[0] for f in os.listdir(figis_path)]
    figis_missing = [os.path.splitext(f)[0] for f in os.listdir(figis_missing_path)]

    for csv_filepath in FileHelpers.files_in_directory_with_path(transactions_path):
        LOG.info("Reading [{}]".format(csv_filepath))
        frame = pandas.read_csv(csv_filepath, index_col=None)
        cusips = frame["cusip"].unique()

        for cusip in cusips:
            if cusip in figis or cusip in figis_missing:
                continue

            LOG.info("Fetching Figi data for [{}]".format(cusip))
            response = OpenFigiService.get_figi(cusip)[0]
            if "error" in response:
                dat = jsonpickle.encode({}, unpicklable=False)
                filepath = os.path.join(figis_missing_path, "{}.json".format(cusip))
            else:
                filepath = os.path.join(figis_path, "{}.json".format(cusip))
                dat = jsonpickle.encode(response["data"][0], unpicklable=False)

            with open(filepath, "w") as output:
                output.write(json.dumps(json.loads(dat), indent=4))


if __name__ == "__main__":
    cli(obj={})
