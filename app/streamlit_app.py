import pandas as pd
import numpy as np
import streamlit as st
import pyarrow.dataset as ds
import duckdb

metrics = [
    "Quantidade de matrículas",
    "Quantidade de escolas"
]

dimensions = {
    "Ano": "NU_ANO_CENSO",
    "Nome da região geográfica": "NO_REGIAO",
	"Nome da Unidade da Federação": "NO_UF",
	"Nome da Mesorregião": "NO_MESORREGIAO",
	"Nome da Microrregião": "NO_MICRORREGIAO"
}

st.cache_resource
def init_connection():
    con = duckdb.connect()
    dataset = ds.dataset("/app/data/transformed.parquet", format="parquet", partitioning="hive")
    con.register("censo", dataset)
    return con


@st.cache_data
def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')


@st.cache_data
def run_query_numero_de_matriculas(dimension: str) -> pd.DataFrame:
    query = f"""
            select
                {dimensions[dimension]} as '{dimension}',
                count(*) as 'Quantidade de matrículas'
            from censo
            group by {dimensions[dimension]}
            order by 1
            """
    return con.execute(query).df()


@st.cache_data
def run_query_numero_de_escolas(dimension: str) -> pd.DataFrame:
    query = f"""
            select
                {dimensions[dimension]} as '{dimension}',
                count(distinct(CO_ENTIDADE)) as 'Quantidade de escolas'
            from censo
            group by {dimensions[dimension]}
            order by 1
            """
    return con.execute(query).df()


"# Censo escolar"

con = init_connection()
metric = st.sidebar.selectbox("Métrica", metrics)
dimension = st.sidebar.selectbox("Dimensão", dimensions.keys())

match metric:
    case "Quantidade de matrículas":
        df = run_query_numero_de_matriculas(dimension)
    case "Quantidade de escolas":
        df = run_query_numero_de_escolas(dimension)
    case _:
        pass

st.bar_chart(df, x=dimension, y=metric, use_container_width=True)
st.dataframe(df)
csv = convert_df(df)
st.download_button(
    label="Download",
    data=csv,
    file_name=f"{metric}_{dimension}.csv",
    mime="text/csv",
)