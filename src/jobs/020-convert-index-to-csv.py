import os
import glob
from helpers.file_helpers import FileHelpers
import luigi

class ParseAllSECFilings(luigi.WrapperTask):
    raw_path = luigi.Parameter(default=FileHelpers.CONFIG["PATH_FILINGS"])

    def requires(self):
        files = glob.glob(os.path.join(self.raw_path, "*"))
        for fh in files:
            yield ParseSECFiling(fh)

class ParseSECFiling(luigi.Task):
    CSVS_PATH = FileHelpers.CONFIG["PATH_FILINGS_CSVS"]
    filepath = luigi.Parameter()

    def run(self):
        csv_lines = [["form", "company", "sic", "date", "path"]]
        with open(self.filepath, encoding='utf-8', errors='replace') as fh:
            lines_buffer = fh.readlines()

            #drop first 10 lines cause they're a constant header
            del lines_buffer[:10]

            #parse fields as fixed width fields
            fieldwidths = (12, 62, 12, 12, 43)
            for line in lines_buffer:
                parser = self.make_parser(fieldwidths)
                fields = [field.strip(' ') for field in parser(line)]
                csv_lines.append(fields)

        with self.output().open('w') as handle:
            writer = csv.writer(handle)
            writer.writerows(csv_lines)

    def output(self):
        output_name = FileHelpers.remove_extension(os.path.basename(self.filepath))
        return luigi.LocalTarget("{}/{}.csv".format(self.CSVS_PATH, output_name))

    def make_parser(self, fieldwidths):
        cuts = tuple(cut for cut in itertools.accumulate(abs(fw) for fw in fieldwidths))
        pads = tuple(fw < 0 for fw in fieldwidths) # bool values for padding fields
        flds = tuple(itertools.zip_longest(pads, (0,)+cuts, cuts))[:-1]  # ignore final one
        parser = lambda line: tuple(line[i:j] for pad, i, j in flds if not pad)

        # optional informational function attributes
        parser.size = sum(abs(fw) for fw in fieldwidths)
        parser.fmtstring = ' '.join('{}{}'.format(abs(fw), 'x' if fw < 0 else 's') for fw in fieldwidths)

        return parser
