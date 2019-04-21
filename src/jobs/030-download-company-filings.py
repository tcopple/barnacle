import os
import urllib.request
import glob
import pandas
import luigi
from helpers.file_helpers import FileHelpers

class FileOutputTask(luigi.ExternalTask):
    filepath = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.filepath)

class DownloadFiling(luigi.ExternalTask):
    local_path = luigi.Parameter()
    remote_path = luigi.Parameter()

    def requires(self):
        pass

    def run(self):
        output = urllib.request.urlopen(self.remote_path).read().decode('utf-8')
        with self.output().open('w') as handle:
            print(output, file=handle)

    def output(self):
        return luigi.LocalTarget(self.local_path)

class DownloadFilings(luigi.Task):
    CSVS_PATH = luigi.Parameter(default=FileHelpers.CONFIG["PATH_FILINGS_CSVS"])
    REPORTS_PATH = FileHelpers.CONFIG["PATH_COMPANY_REPORTS"]

    sic = luigi.Parameter(default="")
    filing_type = luigi.Parameter(default="")
    company_name = luigi.Parameter(default="")

    def requires(self):
        files = glob.glob(os.path.join(self.CSVS_PATH, "*"))
        return [FileOutputTask(fp) for fp in files]

    def run(self):
        frames = []
        for fh in self.input():
            with fh.open('r') as in_file:
                frame = pandas.read_csv(in_file, index_col=None)

                if self.filing_type:
                    frame = frame[frame.form.str.contains(self.filing_type)]

                if self.company_name:
                    frame = frame[frame.company == self.company_name]

                if self.sic:
                    filtered = frame[frame.sic == int(self.sic)]

                frames.append(filtered)

        combined = pandas.concat(frames)
        for index, row in combined.iterrows():
            filename = "{}-{}".format(row["date"].replace("-", ""), row["form"].replace("/", ""))
            company_path = str(int(row["sic"])) + "-" + row["company"][0:10].lower().replace(" ", "").replace("/", "").replace("&", "").replace("(", "").replace(")", "").replace(".", "").replace(",", "")
            local_path = os.path.join(self.REPORTS_PATH, company_path, filename)
            remote_path = "https://www.sec.gov/Archives/{}".format(row["path"])

            yield DownloadFiling(local_path, remote_path)

    def output(self):
        pass
