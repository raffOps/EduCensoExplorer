import pandas as pd
import plotly.express as px
import streamlit as st
from utils import DIMENSOES, DIMENSOES_GEOGRAFICAS, run_query, convert_df, get_valores_possiveis, get_df_filtrado


@st.cache_data
def get_df(
        label_dimensao_geografica: str,
        filtro_dimensao_geografica: str,
        label_dimensao: str
) -> pd.DataFrame:
    query = f"""
                    select
                        NU_ANO_CENSO as 'Ano',
                        {DIMENSOES[label_dimensao]} as '{label_dimensao}',
                        cast(count(distinct(CO_ENTIDADE)) as bigint) as 'Quantidade de escolas'
                    from microdados
                    where 
                        {DIMENSOES_GEOGRAFICAS[label_dimensao_geografica]}='{filtro_dimensao_geografica}'
                        and TP_SITUACAO_FUNCIONAMENTO='Em Atividade'
                        and (
                            QT_MAT_INF > 0 
                            or QT_MAT_FUND > 0 
                            or QT_MAT_MED > 0 
                            or QT_MAT_EJA > 0
                        )
                    group by NU_ANO_CENSO, {DIMENSOES[label_dimensao]}
                    order by 1
                """
    return run_query(query)


def plot(df: pd.DataFrame, tipo_grafico: str, label_dimensao: str, title: str) -> None:
    match tipo_grafico:
        case "Barra":
            fig = px.bar(
                df,
                x="Ano",
                y="Quantidade de escolas",
                color=label_dimensao,
                title=title
            )
        case "Linha":
            fig = px.line(
                df,
                x="Ano",
                y="Quantidade de escolas",
                color=label_dimensao,
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

    df = get_df(label_dimensao_geografica, filtro_dimensao_geografica, label_dimensao)

    filtro_dimensao = st.sidebar.multiselect(
        f"Filtro {label_dimensao}",
        get_valores_possiveis("microdados", DIMENSOES[label_dimensao])
    )

    df = get_df_filtrado(df, label_dimensao, filtro_dimensao)
    title = f"Quantidade de escolas por {label_dimensao} | " \
            f"{label_dimensao_geografica} - {filtro_dimensao_geografica}"
    plot(df, tipo_grafico, label_dimensao, title)
    download(df, title)


if __name__ == "__main__":
    main()
