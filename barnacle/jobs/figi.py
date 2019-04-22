import json
import logging
import os
import sys
import time

import jsonpickle
import luigi
import pandas
import requests

from barnacle.config import BarnacleConfig
from barnacle.helpers.file_helpers import FileHelpers
from barnacle.helpers.logging_helpers import LoggingHelper
from barnacle.services.openfigi_service import OpenFigiService


class FetchCompanyInfo(luigi.Task):
    FIGIS_PATH = BarnacleConfig.PATH_FIGI_DETAILS
    FIGIS_MISSING_PATH = BarnacleConfig.PATH_FIGI_MISSING

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
