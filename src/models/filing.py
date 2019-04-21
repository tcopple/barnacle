class Filing(object):
    def __init__(self, attrs):
        self.type = attrs["type"]
        self.date = attrs["date"]
        self.quarter = attrs["quarter"]
        self.file = attrs["file"]

    def __str__(self):
        return "{}, {}, {}".format(self.type, self.date, self.quarter)
