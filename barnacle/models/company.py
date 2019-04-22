class Company(object):
    @classmethod
    def from_hash(self, attrs):
        name = attrs["company"] if "company" in attrs else attrs["name"]
        sic = str(int(attrs["sic"]))
        filings = []
        return Company(name, sic, filings)

    def __init__(self, name, sic, filings):
        self.name = name
        self.sic = str(int(sic))
        self.filings = filings

    def short_name(self):
        shortened = (
            self.name[0:10]
            .lower()
            .replace(" ", "")
            .replace("/", "")
            .replace("&", "")
            .replace("(", "")
            .replace(")", "")
            .replace(".", "")
        )
        return str(self.sic) + "-" + shortened

    def __str__(self):
        return self.short_name()
