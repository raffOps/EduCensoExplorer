import itertools
import logging
import os
import warnings
from shutil import rmtree
from time import sleep
from zipfile import ZipFile, BadZipfile

import backoff
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("indicadores - extraction")

warnings.filterwarnings("ignore")


def get_url(indicador: str, base: str, year: int) -> str:
    if indicador in {"AFD"}:
        base = base.replace("UFS", "UF")
    if indicador == "TRE":
        if year in {2016, 2017}:
            url = f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{year}/TAXA_REND_{year}_{base.upper()}.zip"
        elif year == 2018:
            url = f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2018/TX_REND_{base.upper()}_2018.zip"
        elif year in range(2019, 2022):
            url = f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{year}/tx_rend_{base.lower()}_{year}.zip"
        else:  # TRE 2022 ainda não está disponível
            raise requests.exceptions.HTTPError(f"{indicador}/{year}")
    else:
        url = f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{year}/{indicador}_{year}_{base.upper()}.zip"
    return url


@backoff.on_exception(
    backoff.expo,
    requests.exceptions.RequestException,
    max_tries=2
)
def make_request(year: int, indicador: str, base: str) -> None:
    url = get_url(indicador, base, year)
    os.makedirs(f"./data/raw/{indicador}/{base}/zips", exist_ok=True)
    with requests.get(url, stream=True, verify=False, timeout=600) as r:
        r.raise_for_status()
        with open(f"./data/raw/{indicador}/{base}/zips/{year}.zip", "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)


def test_zip(year: int, indicador: str, base: str) -> None:
    ZipFile(f"./data/raw/{indicador}/{base}/zips/{year}.zip", "r")


def download_file(year: int, indicador: str, base: str) -> None:
    logger.info(f"Downloading {indicador}/{base}/{year}")
    try:
        make_request(year, indicador, base)
        test_zip(year, indicador, base)

    #  pode acontecer do zip não ser baixado corretamente
    except (requests.exceptions.ChunkedEncodingError, BadZipfile):
        sleep(100)
        if f"./data/raw/{indicador}/{base}/zips/{year}.zip" in os.listdir():
            os.remove(f"./data/raw/{indicador}/{base}/zips/{year}.zip")
        make_request(year, indicador, base)
        test_zip(year, indicador, base)


def unzip_file(year: int, indicador: str, base: str) -> None:
    logger.debug("Unzipping")
    folder = f"data/raw/{indicador}/{base}/{year}"
    with ZipFile(f"./data/raw/{indicador}/{base}/zips/{year}.zip", "r") as zip:
        excel_file = [file for file in zip.namelist() if "xlsx" in file.lower()]
        zip.extractall(folder, members=excel_file)
        os.rename(
            f"{folder}/{excel_file[0]}",
            f"{folder}.xlsx"
        )
        rmtree(folder)

    logger.debug("Unzip complete")


if __name__ == "__main__":
    indicadores = {
        "TRE": "Taxa de Rendimento Escolar",
        "AFD": "Adequação da Formação Docente",
        "IED": "Esforço Docente",
        "ATU": "Média de Alunos por Turma",
        "HAD": "Média de Horas-aula diária",
        "DSU": "Percentual de Docentes com Curso Superior",
        "TDI": "Taxa de Distorção Idade-série",
    }
    bases = ["MUNICIPIOS", "BRASIL_REGIOES_UFS"]
    for year, indicador, base in itertools.product(range(2016, 2023), indicadores, bases):
        try:
            download_file(year, indicador, base)
        except requests.exceptions.HTTPError:  # para ignorar TRE 2022 cujo dataset ainda não existe
            logger.error(f"Download {indicador}/{year} failed")
            continue
        unzip_file(year, indicador, base)
