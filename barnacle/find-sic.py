import csv
import string
import urllib.request
from pprint import pprint

from bs4 import BeautifulSoup

##ENTRY POINT
if __name__ == "__main__":

    data = [["company", "cik", "sic"]]

    suffixes = string.ascii_lowercase
    suffixes = set(string.ascii_lowercase) - set(["u", "v", "w", "x", "y", "z"])
    suffixes = suffixes | set(["uv", "wxyz", "123"])

    for suffix in sorted(suffixes):
        url = (
            "https://www.sec.gov/divisions/corpfin/organization/cfia-" + suffix + ".htm"
        )
        print("Processing [", url, "].")
        with urllib.request.urlopen(url) as html:
            soup = BeautifulSoup(html.read())
            table = soup.find("table", id="cos")
            rows = table.findAll("tr")
            for row in rows:
                cells = row.find_all("td")
                rows = [ele.text.strip() for ele in cells]
                data.append([ele for ele in rows if ele])

    data = [x for x in data if x]
    with open("sics.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerows(data)
