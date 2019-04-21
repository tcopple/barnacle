import json
import time
import requests
import jsonpickle
import sys
import logging
import pandas
import os
import luigi
from services.openfigi_service import OpenFigiService
from helpers.file_helpers import FileHelpers
from helpers.logging_helpers import LoggingHelper

class FetchCompanyInfo(luigi.Task):
    FIGIS_PATH = FileHelpers.CONFIG["PATH_FIGI_DETAILS"]
    FIGIS_MISSING_PATH = FileHelpers.CONFIG["PATH_FIGI_MISSING"]

    cusip = luigi.Parameter(default="")

    def requires(self):
        pass

    def run(self):
        response = OpenFigiService.get_figi(self.cusip)[0]

        if("error" in response):
            dat = jsonpickle.encode({}, unpicklable=False)
        else:
            dat = jsonpickle.encode(response["data"][0], unpicklable=False)

        print(dat)

    def output(self):
        pass
