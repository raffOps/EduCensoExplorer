import duckdb
import pandas as pd
import streamlit as st
from pyarrow import dataset as ds

DIMENSIONS = {
    "Ano": "NU_ANO_CENSO",
    "Nome da região geográfica": "NO_REGIAO",
    "Nome da Unidade da Federação": "NO_UF",
    "Nome da Mesorregião": "NO_MESORREGIAO",
    "Nome da Microrregião": "NO_MICRORREGIAO"
}

@st.cache_resource
def init_db_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    microdados = ds.dataset("data/transformed/microdados.parquet", format="parquet", partitioning="hive")
    con.register("microdados", microdados)
    return con


@st.cache_data
def run_query(query: str) -> pd.DataFrame:
    return con.execute(query).df()


@st.cache_data
def convert_df(df: pd.DataFrame) -> bytes:
    return df.to_csv().encode('utf-8')


con = init_db_connection()