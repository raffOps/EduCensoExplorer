from typing import Any

import pandas as pd
import streamlit as st
import plotly.express as px

from utils import DIMENSIONS, run_query, convert_df

def get_df_dimension(dimension: str) -> pd.DataFrame:
    common_dimensions = {
        "Dependência Administrativa": "TP_DEPENDENCIA",
        "Categoria de escola": "TP_CATEGORIA_ESCOLA_PRIVADA",
        "Localização": "TP_LOCALIZACAO",
        "Localização diferenciada da escola": "TP_LOCALIZACAO_DIFERENCIADA",
        "Nome da Região Geográfica": "NO_REGIAO",
        "Nome da Unidade da Federação": "NO_UF",
        "Nome da Mesorregião": "NO_MESORREGIAO",
        "Nome da Microrregião": "NO_MICRORREGIAO",
        "Nome do Município": "NO_MUNICIPIO"
    }
    column = common_dimensions[dimension]
    query = f"""
                    select
                        NU_ANO_CENSO as 'Ano',
                        {column} as '{dimension}',
                        cast(count(*) as bigint) as 'Quantidade de escolas'
                    from microdados
                    group by NU_ANO_CENSO, {column}
                    order by 1, 2
                """
    return run_query(query)


def get_df_filtred(df: pd.DataFrame, dimension: str) -> pd.DataFrame:
    filter = st.sidebar.multiselect(
        "Filtro",
        df[dimension].unique()
    )
    df = df[df[dimension].isin(filter)]
    return df


def plot(df: pd.DataFrame, dimension: str) -> None:
    tipo_plot = st.sidebar.selectbox(
        "Tipo de gráfico",
        ["Linha", "Barra"]
    )
    match tipo_plot:
        case "Barra":
            fig = px.bar(
                df,
                x='Ano',
                y="Quantidade de escolas",
                color=dimension,
                title=f"Quantidade de escolas por {dimension.lower()}"
            )
        case "Linha":
            fig = px.line(
                df,
                x='Ano',
                y="Quantidade de escolas",
                color=dimension,
                markers=True,
                title=f"Quantidade de escolas por {dimension.lower()}"
            )
        case _:
            fig = None
    st.plotly_chart(fig, use_container_width=True)


def download(df: pd.DataFrame, dimension: str) -> None:
    csv = convert_df(df)
    st.download_button(
        label="Download CSV",
        data=csv,
        file_name=f"quantidade de escolas {dimension}.csv",
        mime="text/csv",
    )


def main() -> None:
    st.markdown("# Censo escolar")
    dimension = st.sidebar.selectbox(
        "Dimensão",
        [
         "Dependência Administrativa",
         "Categoria de escola",
         "Localização",
         "Localização diferenciada da escola"
         "Nome da Região Geográfica",
         "Nome da Unidade da Federação",
         "Nome da Mesorregião",
         "Nome da Microrregião",
         "Nome do Município"]
    )
    
    df = get_df_dimension(dimension)

    df = get_df_filtred(df, dimension)
    plot(df, dimension)
    download(df, dimension)


if __name__ == "__main__":
    main()
