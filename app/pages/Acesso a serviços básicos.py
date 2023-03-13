from typing import Any

import pandas as pd
import streamlit as st
import plotly.express as px

from utils import DIMENSIONS, run_query, convert_df


def get_df(serivce: str, dimension: str) -> pd.DataFrame:
    dimensions = {
        "Dependência Administrativa": "TP_DEPENDENCIA",
        "Categoria de escola": "TP_CATEGORIA_ESCOLA_PRIVADA",
        "Localização": "TP_LOCALIZACAO",
        "Localização diferenciada da escola": "TP_LOCALIZACAO_DIFERENCIADA",
        "Região Geográfica": "NO_REGIAO",
        "Unidade da Federação": "NO_UF",
        "Mesorregião": "NO_MESORREGIAO",
        "Microrregião": "NO_MICRORREGIAO",
        "Município": "NO_MUNICIPIO"
    }

    match serivce:
        case "Abastecimento de água":
            query = f"""
                select
                    NU_ANO_CENSO as 'Ano',
                    {dimensions[dimension]} as '{dimension}',
                    round(cast(count(*) filter (where IN_AGUA_REDE_PUBLICA) as float) / count(*), 3) as 'Rede Pública',
                    round(cast(count(*) filter (where IN_AGUA_POCO_ARTESIANO) as float) / count(*), 3) as 'Poço artesiano',
                    round(cast(count(*) filter (where IN_AGUA_CACIMBA) as float) / count(*), 3) as 'Cacimba/Cisterna/Poço',
                    round(cast(count(*) filter (where IN_AGUA_FONTE_RIO) as float) / count(*), 3) as 'Fonte/Rio/Igarapé/Riacho/Córrego',
                    round(cast(count(*) filter (where IN_AGUA_INEXISTENTE) as float) / count(*), 3) as 'Não há abastecimento de água',
                    round(
                        cast(count(*) filter
                            (where not (IN_AGUA_REDE_PUBLICA or IN_AGUA_POCO_ARTESIANO or IN_AGUA_CACIMBA or IN_AGUA_FONTE_RIO or IN_AGUA_INEXISTENTE ))
                            as float)
                        / count(*)
                    , 3) as 'Sem informações'
                from microdados
                group by NU_ANO_CENSO, {dimensions[dimension]}
                order by 1, 2
            """
        case "Abastecimento de energia elétrica":
            query = f"""
                select
                    NU_ANO_CENSO as 'Ano',
                    {dimensions[dimension]} as '{dimension}',
                    round(cast(count(*) filter (where IN_ENERGIA_REDE_PUBLICA) as float) / count(*), 3) as 'Rede Pública',
                    round(cast(count(*) filter (where IN_ENERGIA_GERADOR_FOSSIL) as float) / count(*), 3) as 'Gerador movido a combustível fóssil',
                    round(cast(count(*) filter (where IN_ENERGIA_RENOVAVEL) as float) / count(*), 3) as 'Fontes de energia renováveis ou alternativas',
                    round(cast(count(*) filter (where IN_ENERGIA_INEXISTENTE) as float) / count(*), 3) as 'Não há energia elétrica',
                    round(
                        cast(count(*) filter
                            (where not (IN_ENERGIA_REDE_PUBLICA or IN_ENERGIA_GERADOR_FOSSIL or IN_ENERGIA_RENOVAVEL or IN_ENERGIA_INEXISTENTE))
                            as float)
                        / count(*)
                    , 3) as 'Sem informações'
                from microdados
                group by NU_ANO_CENSO, {dimensions[dimension]}
                order by 1, 2
            """
        case "Esgoto sanitário":
            query = f"""
                select
                    NU_ANO_CENSO as 'Ano',
                    {dimensions[dimension]} as '{dimension}',
                    round(cast(count(*) filter (where IN_ESGOTO_REDE_PUBLICA) as float) / count(*), 3) as 'Rede Pública',
                    round(cast(count(*) filter (where IN_ESGOTO_FOSSA_SEPTICA) as float) / count(*), 3) as 'Fossa Séptica',
                    round(cast(count(*) filter (where IN_ESGOTO_FOSSA_COMUM) as float) / count(*), 3) as 'Fossa rudimentar/comum',
                    round(cast(count(*) filter (where IN_ESGOTO_FOSSA) as float) / count(*), 3) as 'Fossa',
                    round(cast(count(*) filter (where IN_ESGOTO_INEXISTENTE) as float) / count(*), 3) as 'Não há esgotamento sanitário',
                    round(
                        cast(count(*) filter
                            (where not (IN_ESGOTO_REDE_PUBLICA or IN_ESGOTO_FOSSA_SEPTICA or IN_ESGOTO_FOSSA_COMUM or IN_ESGOTO_FOSSA or IN_ESGOTO_INEXISTENTE ))
                            as float)
                        / count(*)
                    , 3) as 'Sem informações'
                from microdados
                group by NU_ANO_CENSO, {dimensions[dimension]}
                order by 1, 2
            """
        case "Destinação do lixo":
            query = f"""
                select
                    NU_ANO_CENSO as 'Ano',
                    {dimensions[dimension]} as '{dimension}',
                    round(cast(count(*) filter (where IN_LIXO_SERVICO_COLETA) as float) / count(*), 3) as 'Servico de coleta',
                    round(cast(count(*) filter (where IN_LIXO_QUEIMA) as float) / count(*), 3) as 'Queima',
                    round(cast(count(*) filter (where IN_LIXO_ENTERRA) as float) / count(*), 3) as 'Enterra',
                    round(
                            cast(count(*) filter (where IN_LIXO_DESTINO_FINAL_PUBLICO) as float)
                            / count(*),
                        3)
                        as 'Leva a uma destinação final financiada pelo poder público',
                    round(
                            cast(count(*) filter (where IN_LIXO_DESCARTA_OUTRA_AREA) as float)
                            / count(*),
                        3)
                        as 'Destinação do lixo - Descarta em outra área',
                    round(
                        cast(count(*) filter
                            (where not (IN_LIXO_SERVICO_COLETA or IN_LIXO_QUEIMA or IN_LIXO_ENTERRA or IN_LIXO_DESTINO_FINAL_PUBLICO or IN_LIXO_DESCARTA_OUTRA_AREA ))
                            as float)
                        / count(*)
                    , 3) as 'Sem informações'
                from microdados
                group by NU_ANO_CENSO, {dimensions[dimension]}
                order by 1, 2
            """
        case "Acesso a internet":
            query = f"""
                select
                    NU_ANO_CENSO as 'Ano',
                    {dimensions[dimension]} as '{dimension}',
                    round(cast(count(*) filter (where IN_INTERNET) as float) / count(*), 3) as 'Acesso a internet',
                from microdados
                group by NU_ANO_CENSO, {dimensions[dimension]}
                order by 1, 2
            """
    df = run_query(query)
    df = df.melt(
        id_vars=df.columns[:2],
        var_name="Serviço",
        value_name="Taxa de acesso",
        value_vars=df.columns[2:]
    )
    return df


def get_df_filtred(df: pd.DataFrame, service: str, dimension: str) -> pd.DataFrame:
    filter_service = st.sidebar.multiselect(
        "Filtro serviço",
        df["Serviço"].unique()
    )
    filter_dimension = st.sidebar.multiselect(
        "Filtro dimensão",
        df[dimension].unique()
    )
    df = df[df[dimension].isin(filter_dimension) & df["Serviço"].isin(filter_service)]
    return df


def plot(df: pd.DataFrame, service, dimension: str) -> None:
    df["dimensão | serviço"] = df.iloc[:, 1] + " | " + df.iloc[:, 2]
    tipo_plot = st.sidebar.selectbox(
        "Tipo de gráfico",
        ["Linha"]
    )
    match tipo_plot:
        case "Linha":
            fig = px.line(
                df,
                x="Ano",
                y="Taxa de acesso",
                color="dimensão | serviço",
                markers=True,
                title=f"{service} por {dimension.lower()}"
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
    service = st.sidebar.selectbox(
        "Serviço",
        [
            "Abastecimento de água",
            "Abastecimento de energia elétrica",
            "Esgoto sanitário",
            "Destinação do lixo",
            "Acesso a internet"]
    )
    dimension = st.sidebar.selectbox(
        "Dimensão",
        [
            "Região Geográfica",
            "Unidade da Federação",
            "Mesorregião",
            "Microrregião",
            "Município",
            "Dependência Administrativa",
            "Categoria de escola",
            "Localização",
            "Localização diferenciada da escola",
        ]
    )
    df = get_df(service, dimension)
    df = get_df_filtred(df, service, dimension)
    plot(df, service, dimension)
    download(df, dimension)


if __name__ == "__main__":
    main()
