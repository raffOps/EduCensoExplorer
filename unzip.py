import os
import re
import sys
from glob import glob
from time import sleep
from zipfile import ZipFile, BadZipfile
import subprocess
import logging

import requests
import backoff


def unzip_file(year):
    logging.info("Unziping")
    with ZipFile(f"data/zips/microdados_censo_escolar_{year}.zip", 'r') as zip:
        zip.extractall("data")

    recursives_zips = [file for file in glob(f"*{year}/DADOS/*")
                       if ".rar" in file or ".zip" in file]
    for recursive_zip_name in recursives_zips:
        subprocess.run(["unar", "-o", f"{year}/DADOS",  recursive_zip_name])
        os.remove(f"{recursive_zip_name}")

    logging.info("Unzip complete")


if __name__ == "__main__":
    for year in range(2016, 2022):
        #download_file(year)
        unzip_file(year)


