import json
import logging
import os
from shutil import rmtree
import warnings

import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(name="indicadores - transform")

# https://stackoverflow.com/questions/54106638/userwarning-cannot-parse-header-or-footer-so-it-will-be-ignored-on-loading-xl
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')


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


def load_dataframe(indicador: str, year: int, base: str) -> pd.DataFrame:
    if indicador in {"TAP", "TRP", "TAB"}:
        file = f"./data/raw/TRE/{base}/{year}.xlsx"
    else:
        file = f"./data/raw/{indicador}/{base}/{year}.xlsx"
    if not os.path.isfile(file):
        raise FileNotFoundError(file)
    match indicador:
        case "ATU" | "HAD" | "TDI":
            df = pd.read_excel(file, skiprows=8, skipfooter=6, na_values=["--"])
        case "TAP" | "TRP" | "TAB":
            df = pd.read_excel(file, skiprows=8, skipfooter=6, na_values=["--"])
            offset = 7 if base == "MUNICIPIOS" else 4
            match indicador:
                case "TAP":
                    df = pd.concat([df.iloc[:, :offset], df.iloc[:, offset+(18*0):offset+(18*1)]], axis=1)
                case "TRP":
                    df = pd.concat([df.iloc[:, :offset], df.iloc[:, offset+(18*1):offset+(18*2)]], axis=1)
                case "TAB":
                    df = pd.concat([df.iloc[:, :offset], df.iloc[:, offset+(18*2):offset+(18*3)]], axis=1)

        case "DSU":
            df = pd.read_excel(file, skiprows=9, skipfooter=6, na_values=["--"])
        case _:
            df = pd.read_excel(file, skiprows=10, skipfooter=6, na_values=["--"])
    return df


def transform_dataframe(df_municipios: pd.DataFrame, df_brasil: pd.DataFrame, indicador: str) -> pd.DataFrame:
    df_municipios = get_renamed_and_news_columns(df_municipios, indicador, "MUNICIPIOS")
    df_municipios = get_melted_dataframe(df_municipios)
    df_municipios = get_dataframe_with_forced_schema(df_municipios)

    df_brasil = get_renamed_and_news_columns(df_brasil, indicador, "BRASIL_REGIOES_UFS")
    df_brasil = get_melted_dataframe(df_brasil)
    df_brasil = get_dataframe_with_forced_schema(df_brasil)

    return pd.concat([df_municipios, df_brasil], axis=0)


def get_tipo_localidade_geografica(localidade_geografica: str) -> str:
    if localidade_geografica.lower() == "brasil":
        return "País"
    elif localidade_geografica.lower() in {"norte", "centro-oeste", "nordeste", "sul", "sudeste"}:
        return "Região Geográfica"
    else:
        return "Unidade Federativa"


def get_renamed_and_news_columns(df: pd.DataFrame, indicador: str, base: str) -> pd.DataFrame:
    # drop
    if base == "MUNICIPIOS":
        df = df.drop(df.columns[[1, 2, 3]], axis=1)

    # renamed columns
    columns = ['NU_ANO_CENSO', 'NO_LOCALIDADE_GEOGRAFICA', 'NO_CATEGORIA', 'NO_DEPENDENCIA']
    df = df.rename(
        columns=dict(zip(df.columns[:4], columns))
    )
    if len(MAP_INDICADORES[indicador]) != len(df.columns) - 4:
        raise Exception("Quantidade de grupos previamente mapeados não confere com a quantidade de grupos no df")
    df = df.rename(
        columns=dict(zip(df.columns[4:], MAP_INDICADORES[indicador]))
    )
    # new columns
    if base == "MUNICIPIOS":
        df.insert(2, "TP_LOCALIDADE_GEOGRAFICA", "Município")
    else:
        df.insert(
            2,
            "TP_LOCALIDADE_GEOGRAFICA",
            df["NO_LOCALIDADE_GEOGRAFICA"].apply(get_tipo_localidade_geografica)
        )

    return df


def get_melted_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    id_vars = df.columns[:5]
    value_vars = df.columns[5:]
    df = df.melt(id_vars=id_vars, value_vars=value_vars, var_name="TP_GRUPO", value_name="METRICA")
    return df


def get_dataframe_with_forced_schema(df: pd.DataFrame) -> pd.DataFrame:
    integer_columns = ["NU_ANO_CENSO"]
    df[integer_columns] = df[integer_columns].astype("int32")
    df["METRICA"] = df["METRICA"].astype("float")
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
                df_municipios = load_dataframe(indicador, year, "MUNICIPIOS")
                df_brasil = load_dataframe(indicador, year, "BRASIL_REGIOES_UFS")
            except FileNotFoundError:  # TRE 2022
                logger.error(f"{indicador} - {year} failed")
                continue
            df = transform_dataframe(df_municipios, df_brasil, indicador)
            save_dataframe(df, indicador)


if __name__ == "__main__":
    main()
