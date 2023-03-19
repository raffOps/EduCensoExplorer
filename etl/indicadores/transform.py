import json
import logging
import os
from shutil import rmtree

import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(name="indicadores - transform")

INDICADORES = {
    "AFD": "Adequação da Formação Docente",
    "IED": "Esforço Docente",
    "ATU": "Média de Alunos por Turma",
    "HAD": "Média de Horas-aula diária",
    "DSU": "Percentual de Docentes com Curso Superior",
    "TDI": "Taxas de Distorção Idade-série",
    "TAP": "Taxa de Aprovação",  # TRE
    "TRP": "Taxa de Reprovação",  # TRE
    "TAB": "Taxa de Abandono",  # TRE
}


with open("./etl/indicadores/map_indicadores.json") as f:
    MAP_INDICADORES = json.load(f)


def load_dataframe(indicador: str, year: int) -> None:
    if indicador in {"TAP", "TRP", "TAB"}:
        file = f"./data/raw/TRE/{year}.xlsx"
    else:
        file = f"./data/raw/{indicador}/{year}.xlsx"
    if not os.path.isfile(file):
        raise FileNotFoundError(file)
    match indicador:
        case "ATU" | "HAD" | "TDI":
            df = pd.read_excel(file, skiprows=8, skipfooter=6, na_values=["--"])
        case "TAP" | "TRP" | "TAB":
            df = pd.read_excel(file, skiprows=8, skipfooter=6, na_values=["--"])
            match indicador:
                case "TAP":
                    df = df.iloc[:, :27]
                case "TRP":
                    df = pd.concat([df.iloc[:, :9], df.iloc[:, 27:45]], axis=1)
                case "TAB":
                    df = pd.concat([df.iloc[:, :9], df.iloc[:, 45:63]], axis=1)

        case "DSU":
            df = pd.read_excel(file, skiprows=9, skipfooter=6, na_values=["--"])
        case _:
            df = pd.read_excel(file, skiprows=10, skipfooter=6, na_values=["--"])
    return df


def transform_dataframe(df: pd.DataFrame, indicador: str) -> pd.DataFrame:
    df = get_renamed_and_news_columns(df, indicador)
    df = get_melted_dataframe(df)
    df = get_dataframe_with_forced_schema(df, indicador)
    return df


def get_renamed_and_news_columns(df: pd.DataFrame, indicador: str) -> pd.DataFrame:
    # renamed columns
    columns = ['NU_ANO_CENSO', 'NO_REGIAO', 'SG_UF', 'CO_MUNICIPIO', 'NO_MUNICIPIO',
               'CO_ENTIDADE', 'NO_ENTIDADE', 'NO_CATEGORIA', 'NO_DEPENDENCIA']
    df = df.rename(
        columns=dict(zip(df.columns[:9], columns))
    )

    df = df.rename(columns=MAP_INDICADORES[indicador])

    # deleted columns
    df = df.drop(columns=["NO_ENTIDADE"])

    # new columns
    df.insert(8, "NO_INDICADOR", INDICADORES[indicador])
    df.insert(9, "SG_INDICADOR", indicador)
    df.insert(10, "NO_PAIS", "Brasil")

    return df


def get_melted_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    id_vars = df.columns[:11]
    value_vars = df.columns[11:]
    df = df.melt(id_vars=id_vars, value_vars=value_vars, var_name="TP_GRUPO", value_name="METRICA")
    return df


def get_dataframe_with_forced_schema(df: pd.DataFrame, indicador: str) -> pd.DataFrame:
    df[["CO_MUNICIPIO", "CO_ENTIDADE"]] = df[["CO_MUNICIPIO", "CO_ENTIDADE"]].astype("int64")
    df["NU_ANO_CENSO"] = df["NU_ANO_CENSO"].astype("int32")
    return df


def save_dataframe(df: pd.DataFrame, indicador: str) -> None:
    folder = f"./data/transformed/indicadores/{indicador}.parquet"
    df.to_parquet(
        folder,
        engine="pyarrow",
        compression="snappy",
        index=None,
        partition_cols=["NU_ANO_CENSO"]
    )


def main() -> None:
    for indicador in INDICADORES:
        folder = f"./data/transformed/indicadores/{indicador}.parquet"
        if os.path.exists(folder):
            logger.debug(f"Overwriting {folder}")
            rmtree(folder)
        for year in range(2016, 2023):
            logger.info(f"{indicador} - {year}")
            try:
                df = load_dataframe(indicador, year)
            except FileNotFoundError:  # TRE 2022
                logger.error(f"{indicador} - {year} failed")
                continue
            df = transform_dataframe(df, indicador)
            save_dataframe(df, indicador)


if __name__ == "__main__":
    main()
