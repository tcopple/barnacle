import os
import urllib.request
import luigi
from luigi.local_target import LocalTarget
from helpers.file_helpers import FileHelpers
from gluish import BaseTask, Executable, daily, random_string, shellout

class FetchSECIndex(BaseTask):
    URL_BASE = "https://www.sec.gov/Archives/edgar/full-index/"
    FILINGS_PATH = FileHelpers.CONFIG["PATH_FILINGS"]

    quarter = luigi.Parameter()
    year = luigi.Parameter()

    def requires(self):
        pass

    def run(self):
        remote_path = "{}{}/{}/form.idx".format(self.URL_BASE, self.year, self.quarter)
        output = urllib.request.urlopen(remote_path).read().decode('utf-8')
        with self.output().open('w') as handle:
            print(output, file=handle)

    def output(self):
        return luigi.LocalTarget("{}/{}{}.idx".format(self.FILINGS_PATH, self.year, self.quarter))
