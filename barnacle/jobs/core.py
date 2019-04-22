class FileOutputTask(luigi.ExternalTask):
    filepath = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.filepath)
