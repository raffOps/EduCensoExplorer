import pandas as pd
import plotly.express as px
import plotly.graph_objs
import streamlit as st
from utils import DIMENSOES, DIMENSOES_GEOGRAFICAS, run_query, convert_df, get_valores_possiveis, get_df_filtrado


@st.cache_data
def get_df(
        label_dimensao_geografica: str,
        filtro_dimensao_geografica: str,
        label_dimensao: str,
        filtro_dimensao: str
) -> pd.DataFrame:
    if filtro_dimensao == "Total":
        string_filtro_dimensao = ""
    else:
        string_filtro_dimensao = f"and {DIMENSOES[label_dimensao]}='{filtro_dimensao}'"

    query = f"""
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
                where 
                    {DIMENSOES_GEOGRAFICAS[label_dimensao_geografica]}='{filtro_dimensao_geografica}'
                    {string_filtro_dimensao}
                group by NU_ANO_CENSO
                order by 1
            """

    df = run_query(query)
    df = df.melt(
        id_vars=df.columns[:1],
        var_name="Nível de ensino",
        value_name="Quantidade de matrículas",
        value_vars=df.columns[1:]
    )

    return df


@st.cache_data
def plot(df: pd.DataFrame, tipo_grafico: str, title: str) -> None:
    match tipo_grafico:
        case "Barra":
            fig = px.bar(
                df,
                x="Ano",
                y="Quantidade de matrículas",
                color="Nível de ensino",
                title=title
            )
        case "Linha":
            fig = px.line(
                df,
                x="Ano",
                y="Quantidade de matrículas",
                color="Nível de ensino",
                markers=True,
                title=title
            )
        case _:
            fig = None

    st.plotly_chart(fig, use_container_width=True)


def download(df: pd.DataFrame, title: str) -> None:
    csv = convert_df(df)
    st.download_button(
        label="Download CSV",
        data=csv,
        file_name=f"{title}.csv".lower(),
        mime="text/csv",
    )


def main() -> None:
    st.markdown("# Censo escolar")
    tipo_grafico = st.sidebar.selectbox(
        "Tipo de gráfico",
        ["Linha", "Barra"]
    )

    label_dimensao_geografica = st.sidebar.selectbox(
        "Dimensão geográfica",
        DIMENSOES_GEOGRAFICAS.keys()
    )

    filtro_dimensao_geografica = st.sidebar.selectbox(
        f"Filtro {label_dimensao_geografica}",
        get_valores_possiveis("microdados", DIMENSOES_GEOGRAFICAS[label_dimensao_geografica])
    )

    label_dimensao = st.sidebar.selectbox(
        "Dimensão",
        DIMENSOES.keys()
    )

    filtro_dimensao = st.sidebar.selectbox(
        f"Filtro {label_dimensao}",
        ["Total"] + get_valores_possiveis("microdados", DIMENSOES[label_dimensao])
    )

    df = get_df(label_dimensao_geografica, filtro_dimensao_geografica, label_dimensao, filtro_dimensao)

    filtro_nivel_ensino = st.sidebar.multiselect(
        "Nível de ensino",
        df["Nível de ensino"].unique()
    )

    df = get_df_filtrado(df, "Nível de ensino", filtro_nivel_ensino)
    title = f"Quantidade de matrículas | {label_dimensao_geografica} - {filtro_dimensao_geografica} | " \
            f"{label_dimensao} - {filtro_dimensao}"
    plot(df, tipo_grafico, title=title)
    download(df, title)


if __name__ == "__main__":
    main()
