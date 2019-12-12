import luigi
from luigi.contrib.s3 import S3Target, S3Client

class FileOutputTask(luigi.ExternalTask):
    filepath = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.filepath)

class S3OutputTask(luigi.ExternalTask):
    s3_filepath = luigi.Parameter()
    s3_client = luigi.Parameter(default=None)

    def output(self):
        return S3Target(self.s3_filepath, client=self.s3_client)
