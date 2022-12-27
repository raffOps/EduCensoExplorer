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


@backoff.on_exception(
    backoff.expo,
    requests.exceptions.RequestException
)
def make_request(url):
    try:
        with requests.get(url, stream=True, verify=False) as r:
            r.raise_for_status()
            with open(f"data/zips/{year}.zip", 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
    except Exception as e:
        print(e)


def test_zip(year):
    ZipFile(f"data/zips/{year}.zip", 'r')


def download_file(year):
    url = f"https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_{year}.zip"
    logging.info(f"Downloading {url}")
    try:
        make_request(url)
        test_zip(year)
    except (requests.exceptions.ChunkedEncodingError, BadZipfile) as e:
        sleep(100)
        try:
            if f"data/zips/{year}.zip" in os.listdir():
                os.remove(f"{year}.zip")
            make_request(url)
            test_zip(year)
        except Exception as e:
            raise Exception(f"Download error: {e}")
    except Exception as e:
        raise Exception(f"Download error: {e}")

    logging.info("Download complete")


def unzip_file(year):
    logging.info("Unziping")
    with ZipFile(f"data/zips/{year}.zip", 'r') as zip:
        zip.extractall("data")

    recursives_zips = [file for file in glob(f"*{year}/DADOS/*")
                       if ".rar" in file or ".zip" in file]
    for recursive_zip_name in recursives_zips:
        subprocess.run(["unar", "-o", f"{year}/DADOS", recursive_zip_name])
        os.remove(f"{recursive_zip_name}")

    logging.info("Unzip complete")


if __name__ == "__main__":
    for year in range(2016, 2022):
        download_file(year)
        unzip_file(year)
