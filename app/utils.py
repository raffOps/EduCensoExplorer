import json

import duckdb
import pandas as pd
import streamlit as st
from pyarrow import dataset as ds

DIMENSOES = {
    "Dependência Administrativa": "TP_DEPENDENCIA",
    "Categoria de escola": "TP_CATEGORIA_ESCOLA_PRIVADA",
    "Localização": "TP_LOCALIZACAO",
    "Localização diferenciada da escola": "TP_LOCALIZACAO_DIFERENCIADA",
    "Região Geográfica": "NO_REGIAO",
    "Sigla da Unidade da Federação": "SG_UF",
    "Mesorregião": "NO_MESORREGIAO",
    "Microrregião": "NO_MICRORREGIAO",
    "Município": "NO_MUNICIPIO"
}

@st.cache_resource
def init_db_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    microdados = ds.dataset("data/transformed/microdados.parquet", format="parquet", partitioning="hive")
    con.register("microdados", microdados)
    return con

def load_geojson(level: str) -> dict:
    with open(f"data/geo/{level}.json", encoding="latin1") as f:
        uf_json = json.load(f)

@st.cache_data
def run_query(query: str) -> pd.DataFrame:
    return con.execute(query).df()


@st.cache_data
def convert_df(df: pd.DataFrame) -> bytes:
    return df.to_csv().encode('utf-8')


con = init_db_connection()