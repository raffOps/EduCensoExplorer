import pandas as pd
import plotly.express as px
import plotly.graph_objs
import streamlit as st
from utils import DIMENSOES, run_query, convert_df


@st.cache_data
def get_df_nivel_ensino() -> pd.DataFrame:
    query = """
                select
                    NU_ANO_CENSO as 'Ano',
                    cast(sum(QT_MAT_BAS) as bigint) as 'Educação Básica',
                    cast(sum(QT_MAT_INF_CRE) as bigint) as 'Educação Infantil - creche',
                    cast(sum(QT_MAT_INF_PRE) as bigint) as 'Educação Infantil - pré-escola',
                    cast(sum(QT_MAT_INF) as bigint) as 'Educação Infantil',
                    cast(sum(QT_MAT_FUND_AI) as bigint) as 'Ensino Fundamental - anos iniciais',
                    cast(sum(QT_MAT_FUND_AF) as bigint) as 'Ensino Fundamental - anos finais',
                    cast(sum(QT_MAT_FUND) as bigint) as 'Ensino Fundamental',
                    cast(sum(QT_MAT_MED) as bigint) as 'Ensino Médio',
                    cast(sum(QT_MAT_PROF) as bigint) as 'Educação Profissional',
                    cast(sum(QT_MAT_PROF_TEC) as bigint) as 'Educação Profissional Técnica',
                    cast(sum(QT_MAT_EJA_FUND) as bigint) as 'Educação de Jovens e Adultos (EJA) - Ensino Fundamental',
                    cast(sum(QT_MAT_EJA_MED) as bigint) as 'Educação de Jovens e Adultos (EJA) - Ensino Médio',
                    cast(sum(QT_MAT_EJA) as bigint) as 'Educação de Jovens e Adultos (EJA)',
                    cast(sum(QT_MAT_ESP_CC) as bigint) as 'Educação Especial Inclusiva',
                    cast(sum(QT_MAT_ESP_CE) as bigint) as 'Educação Especial Exclusiva',
                    cast(sum(QT_MAT_ESP) as bigint) as 'Educação Especial'
                from microdados
                group by NU_ANO_CENSO
                order by 1
            """
    df = run_query(query)
    df = df.melt(
        id_vars='Ano',
        var_name="Nível de ensino",
        value_name="Quantidade de matrículas",
        value_vars=df.columns[1:]
    )

    return df


@st.cache_data
def get_df_dimensao(dimensao: str) -> pd.DataFrame:
    query = f"""
                    select
                        NU_ANO_CENSO as 'Ano',
                        {DIMENSOES[dimensao]} as '{dimensao}',
                        cast(sum(QT_MAT_BAS) as bigint) as 'Quantidade de matrículas'
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
                y="Quantidade de matrículas",
                color=dimensao,
                title=f"Quantidade de matrículas por {dimensao.lower()}"
            )
        case "Linha":
            fig = px.line(
                df,
                x='Ano',
                y="Quantidade de matrículas",
                color=dimensao,
                markers=True,
                title=f"Quantidade de matrículas por {dimensao.lower()}"
            )
        case _:
            fig = None
    return fig


def download(df: pd.DataFrame, dimensao: str) -> None:
    csv = convert_df(df)
    st.download_button(
        label="Download CSV",
        data=csv,
        file_name=f"quantidade de matriculas {dimensao.lower()}.csv",
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
    if dimensao == "Nível de ensino":
        df = get_df_nivel_ensino()
    else:
        df = get_df_dimensao(dimensao)

    filtro = st.sidebar.multiselect(
        "Filtro dimensao",
        df[dimensao].unique()
    )

    df = get_df_filtrado(df, dimensao, filtro)
    plot(df, tipo_grafico, dimensao)
    download(df, dimensao)


if __name__ == "__main__":
    main()
