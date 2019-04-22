import json
import logging
import os
import sys
import time

import jsonpickle
import pandas
import requests

from barnacle.helpers.file_helpers import FileHelpers
from barnacle.helpers.logging_helpers import LoggingHelper
from barnacle.services.openfigi_service import OpenFigiService

##ENTRY POINT
if __name__ == "__main__":

    LoggingHelper.setup()
    log = logging.getLogger(__name__)

    transactions_path = FileHelpers.CONFIG["PATH_TRANSACTIONS"]
    figis_path = FileHelpers.CONFIG["PATH_FIGI_DETAILS"]
    figis_missing_path = FileHelpers.CONFIG["PATH_FIGI_MISSING"]
    FileHelpers.try_make_directory(figis_path)
    FileHelpers.try_make_directory(figis_missing_path)
    figis = [os.path.splitext(f)[0] for f in os.listdir(figis_path)]
    figis_missing = [os.path.splitext(f)[0] for f in os.listdir(figis_missing_path)]

    for csv_filepath in FileHelpers.files_in_directory_with_path(transactions_path):
        log.info("Reading [{}]".format(csv_filepath))
        frame = pandas.read_csv(csv_filepath, index_col=None)
        cusips = frame["cusip"].unique()

        for cusip in cusips:
            if cusip in figis or cusip in figis_missing:
                continue

            log.info("Fetching Figi data for [{}]".format(cusip))
            response = OpenFigiService.get_figi(cusip)[0]
            if "error" in response:
                dat = jsonpickle.encode({}, unpicklable=False)
                filepath = os.path.join(figis_missing_path, "{}.json".format(cusip))
            else:
                filepath = os.path.join(figis_path, "{}.json".format(cusip))
                dat = jsonpickle.encode(response["data"][0], unpicklable=False)

            with open(filepath, "w") as output:
                output.write(json.dumps(json.loads(dat), indent=4))
