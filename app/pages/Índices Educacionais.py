from typing import Any

import pandas as pd
import streamlit as st
import plotly.express as px

from utils import DIMENSIONS, run_query, convert_df


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
                        cast(sum(QT_MAT_BAS) as bigint) as 'Quantidade de matrículas'
                    from microdados
                    group by NU_ANO_CENSO, {column}
                    order by 1, 2
                """
    return run_query(query)


def get_df_filtrado(df: pd.DataFrame, dimension: str) -> pd.DataFrame:
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
                y="Quantidade de matrículas",
                color=dimension,
                title=f"Quantidade de matrículas por {dimension.lower()}"
            )
        case "Linha":
            fig = px.line(
                df,
                x='Ano',
                y="Quantidade de matrículas",
                color=dimension,
                markers=True,
                title=f"Quantidade de matrículas por {dimension.lower()}"
            )
        case _:
            fig = None
    st.plotly_chart(fig, use_container_width=True)


def download(df: pd.DataFrame, dimension: str) -> None:
    csv = convert_df(df)
    st.download_button(
        label="Download CSV",
        data=csv,
        file_name=f"quantidade de matriculas {dimension}.csv",
        mime="text/csv",
    )


def main() -> None:
    st.markdown("# Censo escolar")
    dimension = st.sidebar.selectbox(
        "Dimensão",
        ["Nível de ensino",
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
    if dimension == "Nível de ensino":
        df = get_df_nivel_ensino()
    else:
        df = get_df_dimension(dimension)

    df = get_df_filtrado(df, dimension)
    plot(df, dimension)
    download(df, dimension)


if __name__ == "__main__":
    pass
