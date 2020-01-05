import itertools
import logging
import os
import shutil
import urllib

from typing import Callable, List

LOG = logging.getLogger(__name__)


class FileHelpers(object):
    @classmethod
    def download_file(cls, remote_path: str, local_path: str) -> None:
        LOG.info(f"Downloading file from [{remote_path}] to [{local_path}].")

        # if directory of local path does not exist, create it
        path = os.path.dirname(local_path)

        if not os.path.exists(path):
            LOG.info(f"Making directory [{path}]")
            os.makedirs(path)

        # fetch remote file storying it piecewise to local file
        with urllib.request.urlopen(remote_path) as response, open(
            local_path, "wb"
        ) as local:
            shutil.copyfileobj(response, local)

    @classmethod
    def change_file_extension(cls, filepath: str, new_suffix: str) -> str:
        base_filepath = os.path.splitext(filepath)[0].lower()
        return f"{filepath_no_ext}.{new_suffix}"

    @classmethod
    def try_make_directory(cls, path: str) -> bool:
        if not os.path.exists(path):
            os.makedirs(path)
            return True

        return False

    @classmethod
    def try_make_directory_for_file(cls, filepath: str) -> bool:
        path = os.path.dirname(filepath)
        return cls.try_make_directory(path)

    @classmethod
    def files_in_directory_with_path(cls, path: str) -> List[str]:
        return [os.path.join(path, f) for f in os.listdir(path)]

    @classmethod
    def make_fixed_width_parser(cls, fieldwidths: str) -> Callable:
        cuts = tuple(cut for cut in itertools.accumulate(abs(fw) for fw in fieldwidths))
        pads = tuple(fw < 0 for fw in fieldwidths)  # bool values for padding fields
        flds = tuple(itertools.zip_longest(pads, (0,) + cuts, cuts))[
            :-1
        ]  # ignore final one
        parser = lambda line: tuple(line[i:j] for pad, i, j in flds if not pad)

        # optional informational function attributes
        parser.size = sum(abs(fw) for fw in fieldwidths)
        parser.fmtstring = " ".join(
            "{}{}".format(abs(fw), "x" if fw < 0 else "s") for fw in fieldwidths
        )

        return parser

    @classmethod
    def remove_extension(cls, filepath: str) -> str:
        filepath, ext =  os.path.splitext(filename)
        return filename
