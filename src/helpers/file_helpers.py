import os
import urllib
import sys
import shutil
import logging
import json
import itertools

class FileHelpers(object):

    log = logging.getLogger(__name__)
    config_path = "config.json"

    @classmethod
    def download_file(cls, remote_path, local_path):
        cls.log.info("Downloading file from [%s] to [%s]", remote_path, local_path)
        #if directory of local path does not exist, create it
        path = os.path.dirname(local_path)

        if not os.path.exists(path):
            cls.log.info("Making directory [%s]", path)
            os.makedirs(path)

        #fetch remote file storying it piecewise to local file
        with urllib.request.urlopen(remote_path) as response, open(local_path, 'wb') as local:
            shutil.copyfileobj(response, local)

        cls.log.info("Success...")

    @classmethod
    def app_root(cls):
        if(cls.CONFIG == None):
            if os.path.exists(cls.config_path):
                cls.log.info("Loading configuration file [{}]".format(cls.config_path))
                with open(cls.config_path, 'rt') as f:
                    cls.CONFIG = json.load(f)

        return cls.CONFIG["DIR_ROOT"] if "DIR_ROOT" in cls.CONFIG else "./"

    @classmethod
    def change_file_extension(cls, filepath, new_suffix):
        filepath_no_ext = os.path.splitext(filepath)[0].lower()
        ret = "{}.{}".format(filepath_no_ext, new_suffix)

        return ret

    @classmethod
    def try_make_directory(cls, path):
        if not os.path.exists(path):
            os.makedirs(path)

    @classmethod
    def try_make_directory_for_file(cls, filepath):
        path = os.path.dirname(filepath)
        cls.try_make_directory(path)

    @classmethod
    def files_in_directory_with_path(cls, d):
        return [os.path.join(d, f) for f in os.listdir(d)]

    @classmethod
    def make_fixed_width_parser(cls, fieldwidths):
        cuts = tuple(cut for cut in itertools.accumulate(abs(fw) for fw in fieldwidths))
        pads = tuple(fw < 0 for fw in fieldwidths) # bool values for padding fields
        flds = tuple(itertools.zip_longest(pads, (0,)+cuts, cuts))[:-1]  # ignore final one
        parser = lambda line: tuple(line[i:j] for pad, i, j in flds if not pad)

        # optional informational function attributes
        parser.size = sum(abs(fw) for fw in fieldwidths)
        parser.fmtstring = ' '.join('{}{}'.format(abs(fw), 'x' if fw < 0 else 's') for fw in fieldwidths)

        return parser

    @classmethod
    def remove_extension(cls, filename):
        return os.path.splitext(filename)[0]

    def setup_config(filepath, log):
        ret = {}
        if os.path.exists(filepath):
            log.info("Loading configuration file [{}]".format(filepath))
            with open(filepath, 'rt') as f:
                ret = json.load(f)

        return ret

    CONFIG = setup_config(config_path, log)
