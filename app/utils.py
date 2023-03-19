import duckdb
import pandas as pd
import streamlit as st
from pyarrow import dataset as ds
INDICADORES = {
    "Adequação da Formação Docente": "AFD",
    "Esforço Docente": "IED",
    "Média de Alunos por Turma": "ATU",
    "Média de Horas-aula diária": "HAD",
    "Percentual de Docentes com Curso Superior": "DSU",
    "Taxas de Distorção Idade-série": "TDI"
}

DIMENSOES = {
    "Dependência Administrativa": "TP_DEPENDENCIA",
    "Categoria de escola": "TP_CATEGORIA_ESCOLA_PRIVADA",
    "Localização": "TP_LOCALIZACAO",
}

DIMENSOES_GEOGRAFICAS = {
    "País": "NO_PAIS",
    "Região Geográfica": "NO_REGIAO",
    "Unidade da Federação": "SG_UF",
    "Mesorregião": "NO_MESORREGIAO",
    "Microrregião": "NO_MICRORREGIAO",
    "Município": "NO_MUNICIPIO"
}



@st.cache_resource
def init_db_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    microdados = ds.dataset("data/transformed/microdados.parquet", format="parquet", partitioning="hive")
    con.register("microdados", microdados)

    indicadores = {}
    for indicador in INDICADORES.values():
        indicadores[indicador] = ds.dataset(f"data/transformed/indicadores/{indicador}.parquet", format="parquet",
                                            partitioning="hive")
        con.register(indicador, indicadores[indicador])
    return con


@st.cache_data
def run_query(query: str) -> pd.DataFrame:
    return con.execute(query).df()


@st.cache_data
def get_valores_possiveis(fonte: str, coluna: str) -> list[str]:
    if fonte == "microdados":
        query = f"select distinct({coluna}) as value from microdados order by 1"
    else:
        query = f"select distinct({coluna}) as value from {INDICADORES[fonte]} order by 1"

    return run_query(query)["value"].tolist()


@st.cache_data
def get_df_filtrado(df: pd.DataFrame, dimensao: str, filtro: list[str]) -> pd.DataFrame:
    if filtro:
        if isinstance(filtro, str):
            filtro = [filtro]
        df = df[df[dimensao].isin(filtro)]
    return df


@st.cache_data
def convert_df(df: pd.DataFrame) -> bytes:
    return df.to_csv().encode('utf-8')


con = init_db_connection()
