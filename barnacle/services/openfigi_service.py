import json
import logging
import sys

import requests

from barnacle.config import BarnacleConfig

LOG = logging.getLogger(__name__)


class OpenFigiService(object):
    openfigi_headers = {
        "Content-Type": "text/json",
        "X-OPENFIGI-APIKEY": BarnacleConfig.OPENFIGI_APIKEY,
    }

    @staticmethod
    def get_figis(cusips):
        ret = {}
        for cusip in cusips:
            ret[cusip] = OpenFigiService.get_figi(cusip)

        return ret

    @staticmethod
    def get_figi(cusip):
        jobs = [{"idType": "ID_CUSIP", "idValue": cusip, "exchCode": "US"}]

        response = requests.post(
            url=BarnacleConfig.OPENFIGI_URL,
            headers=OpenFigiService.openfigi_headers,
            data=json.dumps(jobs),
        )

        OpenFigiService.log.info(
            "Recieved [{}] from OpenFigi on [{}]: [{}]".format(
                response.status_code, cusip, response.text
            )
        )

        if response.status_code != 200:
            OpenFigiService.log.info(
                "Failed to fetch figi for {} status code [{}]".format(
                    cusip, response.status_code
                )
            )

            return {}

        return response.json()

    @staticmethod
    def get_figis(cusips):
        jobs = [
            {"idType": "ID_CUSIP", "idValue": cusip, "exchCode": "US"}
            for cusip in cusips
        ]

        response = requests.post(
            url=BarnacleConfig.OPENFIGI_URL,
            headers=OpenFigiService.openfigi_headers,
            data=json.dumps(jobs),
        )

        if response.status_code != 200:
            LOG.info(
                "Failed to fetch figis with status code [{}]".format(
                    response.status_code
                )
            )

            return {}

        ret = []
        for job, response in zip(jobs, response.json()):
            ret.append({'cusip': job.get('idValue'), **response})

        return ret
