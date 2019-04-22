import json
import logging
import sys

import requests


class OpenFigiService(object):
    openfigi_url = "https://api.openfigi.com/v1/mapping"
    openfigi_apikey = "4a455c69-3894-4542-b1ad-4584f102b36b"
    openfigi_headers = {
        "Content-Type": "text/json",
        "X-OPENFIGI-APIKEY": openfigi_apikey,
    }
    log = logging.getLogger(__name__)

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
            url=OpenFigiService.openfigi_url,
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
