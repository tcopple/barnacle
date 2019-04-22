class Filing(object):
    def __init__(self, type, date, quarter, file):
        self.type = type
        self.date = date
        self.quarter = quarter
        self.file = file

    def __str__(self):
        return "{}, {}, {}".format(self.type, self.date, self.quarter)
