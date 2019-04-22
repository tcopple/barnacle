class Company(object):
    @classmethod
    def from_hash(self, attrs):
        name = attrs.get("company") or attrs.get("name")
        sic = str(int(attrs["sic"]))
        filings = []

        return Company(name, sic, filings)

    def __init__(self, name, sic, filings=None):
        self.name = name
        self.sic = sic
        self.filings = filings or []

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
