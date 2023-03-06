import os
from time import sleep
from zipfile import ZipFile, BadZipfile
import logging

import requests
import backoff
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("etl - indicadores")

@backoff.on_exception(
    backoff.expo,
    requests.exceptions.RequestException
)
def make_request(year: int, indicador: str) -> None:
    url = f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{year}/{indicador}_{year}_ESCOLAS.zip"
    with requests.get(url, stream=True, verify=False) as r:
        r.raise_for_status()
        with open(f"./data/raw/{indicador}/zips/{year}.zip", 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)


def test_zip(year: int, indicador: str) -> None:
    ZipFile(f"./data/raw/{indicador}/zips/{year}.zip", 'r')


def download_file(year: int, indicador: str) -> None:
    logger.info(f"Downloading {indicador}/{year}")
    try:
        make_request(year, indicador)
        test_zip(year, indicador)
    except (requests.exceptions.ChunkedEncodingError, BadZipfile) as e:
        sleep(100)
        try:
            if f"./data/raw/{indicador}/zips/{year}.zip" in os.listdir():
                os.remove(f"./data/raw/{indicador}/zips/{year}.zip")
            make_request(year, indicador)
            test_zip(year, indicador)
        except Exception as e:
            raise Exception(f"Download error: {e}") from e
    except Exception as e:
        raise Exception(f"Download error: {e}") from e

    logger.debug("Download complete")


def unzip_file(year: int, indicador: str) -> None:
    logger.debug("Unzipping")
    with ZipFile(f"./data/raw/{indicador}/zips/{year}.zip", 'r') as zip:
        zip.extractall(f"data/raw/{indicador}")

    logger.debug("Unzip complete")


if __name__ == "__main__":
    indicadores = {
        "AFD": "Adequação da Formação Docente",
        "ICG": "Complexidade de Gestão da Escola",
        "IED": "Esforço Docente",
        "ATU": "Média de Alunos por Turma",
        "HAD": "Média de Horas-aula diária",
        "DSU": "Percentual de Docentes com Curso Superior",
        "TDI": "Taxas de Distorção Idade-série"
    }
    for year in range(2016, 2022):
        for indicador in indicadores:
            download_file(year, indicador)
            unzip_file(year, indicador)
