import pandas as pd
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


@st.cache_resource
def init_db_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    dataset = ds.dataset("data/transformed.parquet", format="parquet", partitioning="hive")
    con.register("censo", dataset)
    return con


@st.cache_data
def run_query(query: str) -> pd.DataFrame:
    return con.execute(query).df()


@st.cache_data
def convert_df(df: pd.DataFrame) -> bytes:
    return df.to_csv().encode('utf-8')


def main() -> None:
    "# Censo escolar"

    metric = st.sidebar.selectbox("Métrica", metrics)
    dimension = st.sidebar.selectbox("Dimensão", dimensions.keys())

    f"{metric.upper()} x {dimension.upper()}"

    match metric:
        case "Quantidade de matrículas":
            query = f"""
                    select
                        {dimensions[dimension]} as '{dimension}',
                        count(*) as 'Quantidade de matrículas'
                    from censo
                    group by {dimensions[dimension]}
                    order by 1
                    """
        case "Quantidade de escolas":
            query = f"""
                    select
                        {dimensions[dimension]} as '{dimension}',
                        count(distinct(CO_ENTIDADE)) as 'Quantidade de escolas'
                    from censo
                    group by {dimensions[dimension]}
                    order by 1
                    """
        case _:
            query = None

    df = run_query(query)
    st.bar_chart(df, x=dimension, y=metric, use_container_width=True)
    st.dataframe(df)
    csv = convert_df(df)
    st.download_button(
        label="Download",
        data=csv,
        file_name=f"{metric}_{dimension}.csv",
        mime="text/csv",
    )


if __name__ == "__main__":
    con = init_db_connection()
    main()
