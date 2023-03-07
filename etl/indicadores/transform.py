import logging
import os
import itertools
import json

import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(name="indicadores - transform")

INDICADORES = {
    #"AFD": "Adequação da Formação Docente",
    #"ICG": "Complexidade de Gestão da Escola",
    #"IED": "Esforço Docente",
    #"ATU": "Média de Alunos por Turma",
    #"HAD": "Média de Horas-aula diária",
    #"DSU": "Percentual de Docentes com Curso Superior",
    "TDI": "Taxas de Distorção Idade-série"
}

with open("./etl/indicadores/map_indicadores.json") as file:
    MAP_INDICADORES = json.load(file)


def get_dataframe(indicador, year):
    file = f"./data/raw/{indicador}/{indicador}_{year}_ESCOLAS/{indicador}_ESCOLAS_{year}.xlsx"
    if not os.path.isfile(file):
        file = f"./data/raw/{indicador}/{indicador}_{year}_ESCOLAS/{indicador}_ESCOLAS_{year}_ATUALIZADO.xlsx"
        if not os.path.isfile(file):
            raise FileNotFoundError(file)
    try:
        match indicador:
            case "ATU" | "HAD" | "TDI":
                df = pd.read_excel(file, skiprows=8, skipfooter=6, na_values=["--"])
            case "DSU":
                df = pd.read_excel(file, skiprows=9, skipfooter=6, na_values=["--"])
            case _:
                df = pd.read_excel(file, skiprows=10, skipfooter=6, na_values=["--"])
        return df
    except FileNotFoundError as e:
        raise FileNotFoundError(file) from e


def transform_dataframe(df: pd.DataFrame, indicador: str) -> pd.DataFrame:
    # rename columns
    columns = ['NU_ANO_CENSO', 'NO_REGIAO', 'SG_UF', 'CO_MUNICIPIO', 'NO_MUNICIPIO',
       'CO_ENTIDADE', 'NO_ENTIDADE', 'NO_CATEGORIA', 'NO_DEPENDENCIA']
    df = df.rename(
        columns=dict(zip(df.columns[:9], columns))
    )
    df = get_melted_dataframe(df)
    df = map_indicadores(df, indicador)
    df = force_schema(df)
    df = df.drop(columns=["NO_ENTIDADE"])
    return df


def get_melted_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    id_vars = df.columns[:9]
    value_vars = df.columns[9:]
    df = df.melt(id_vars=id_vars, value_vars=value_vars, var_name="TP_GRUPO", value_name="METRICA")
    return df


def force_schema(df: pd.DataFrame) -> pd.DataFrame:
    df[["CO_MUNICIPIO", "CO_ENTIDADE"]] = df[["CO_MUNICIPIO", "CO_ENTIDADE"]].astype("int64")
    df["NU_ANO_CENSO"] = df["NU_ANO_CENSO"].astype("int32")
    return df


def map_indicadores(df: pd.DataFrame, indicador) -> pd.DataFrame:
    df["TP_GRUPO"] = df["TP_GRUPO"].replace(MAP_INDICADORES[indicador])
    df["NO_INDICADOR"] = INDICADORES[indicador]
    df["SG_INDICADOR"] = indicador
    return df


def save_to_parquet(df: pd.DataFrame, indicador) -> None:
    df.to_parquet(
        f"./data/transformed/indicadores/{indicador}.parquet",
        engine="pyarrow",
        compression="snappy",
        index=None,
        partition_cols=["NU_ANO_CENSO"]
    )


def main():
    for indicador, year in itertools.product(INDICADORES, range(2016, 2022)):
        logger.info(f"{indicador} - {year}")
        df = get_dataframe(indicador, year)
        df = transform_dataframe(df, indicador)
        save_to_parquet(df, indicador)


if __name__ == "__main__":
    main()
