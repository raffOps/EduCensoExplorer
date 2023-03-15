import pandas as pd
import plotly.express as px
import plotly.graph_objs
import streamlit as st
from utils import DIMENSOES, run_query, convert_df


@st.cache_data
def get_df_dimensao(dimensao: str) -> pd.DataFrame:
    query = f"""
                    select
                        NU_ANO_CENSO as 'Ano',
                        {DIMENSOES[dimensao]} as '{dimensao}',
                        cast(count(*) as bigint) as 'Quantidade de escolas'
                    from microdados
                    group by NU_ANO_CENSO, {DIMENSOES[dimensao]}
                    order by 1, 2
                """
    return run_query(query)


@st.cache_data
def get_df_filtrado(df: pd.DataFrame, dimensao: str, filtro: list[str]) -> pd.DataFrame:
    if filtro:
        df = df[df[dimensao].isin(filtro)]
    return df


def plot(df: pd.DataFrame, tipo_grafico: str, dimensao: str) -> None:
    fig = get_fig(df, dimensao, tipo_grafico)
    st.plotly_chart(fig, use_container_width=True)


@st.cache_data
def get_fig(df: pd.DataFrame, dimensao: str, tipo_grafico: str) -> plotly.graph_objs.Figure:
    match tipo_grafico:
        case "Barra":
            fig = px.bar(
                df,
                x='Ano',
                y="Quantidade de escolas",
                color=dimensao,
                title=f"Quantidade de escolas por {dimensao.lower()}"
            )
        case "Linha":
            fig = px.line(
                df,
                x='Ano',
                y="Quantidade de escolas",
                color=dimensao,
                markers=True,
                title=f"Quantidade de escolas por {dimensao.lower()}"
            )
        case _:
            fig = None
    return fig


def download(df: pd.DataFrame, dimensao: str) -> None:
    csv = convert_df(df)
    st.download_button(
        label="Download CSV",
        data=csv,
        file_name=f"quantidade de escolas {dimensao.lower()}.csv",
        mime="text/csv",
    )


def main() -> None:
    st.markdown("# Censo escolar")
    tipo_grafico = st.sidebar.selectbox(
        "Tipo de gráfico",
        ["Linha", "Barra"]
    )
    dimensao = st.sidebar.selectbox(
        "Dimensão",
        DIMENSOES
    )

    df = get_df_dimensao(dimensao)

    filtro = st.sidebar.multiselect(
        "Filtro dimensão",
        df[dimensao].unique()
    )
    df = get_df_filtrado(df, dimensao, filtro)
    plot(df, tipo_grafico, dimensao)
    download(df, dimensao)


if __name__ == "__main__":
    main()
