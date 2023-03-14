from typing import Any
import json

import pandas as pd
import streamlit as st
import plotly.express as px

from utils import DIMENSOES, run_query, convert_df, load_geojson

@st.cache_data
def get_df(servico: str, dimensao: str) -> pd.DataFrame:
    match servico:
        case "Abastecimento de água":
            query = f"""
                select
                    NU_ANO_CENSO as 'Ano',
                    {DIMENSOES[dimensao]} as '{dimensao}',
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
                group by NU_ANO_CENSO, {DIMENSOES[dimensao]}
                order by 1, 2
            """
        case "Abastecimento de energia elétrica":
            query = f"""
                select
                    NU_ANO_CENSO as 'Ano',
                    {DIMENSOES[dimensao]} as '{dimensao}',
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
                group by NU_ANO_CENSO, {DIMENSOES[dimensao]}
                order by 1, 2
            """
        case "Esgoto sanitário":
            query = f"""
                select
                    NU_ANO_CENSO as 'Ano',
                    {DIMENSOES[dimensao]} as '{dimensao}',
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
                group by NU_ANO_CENSO, {DIMENSOES[dimensao]}
                order by 1, 2
            """
        case "Destinação do lixo":
            query = f"""
                select
                    NU_ANO_CENSO as 'Ano',
                    {DIMENSOES[dimensao]} as '{dimensao}',
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
                group by NU_ANO_CENSO, {DIMENSOES[dimensao]}
                order by 1, 2
            """
        case "Acesso a internet":
            query = f"""
                select
                    NU_ANO_CENSO as 'Ano',
                    {DIMENSOES[dimensao]} as '{dimensao}',
                    round(cast(count(*) filter (where IN_INTERNET) as float) / count(*), 3) as 'Acesso a internet',
                from microdados
                group by NU_ANO_CENSO, {DIMENSOES[dimensao]}
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


@st.cache_data
def get_df_filtrado(df: pd.DataFrame, filtro_servico: str) -> pd.DataFrame:
    df = df[df["Serviço"].isin([filtro_servico])]
    df["Nível de acesso em %"] = df["Taxa de acesso"] * 100
    return df


def plot(df: pd.DataFrame, tipo_grafico, servico: str, filtro_servico: str, dimensao: str) -> None:
    fig = get_fig(df, tipo_grafico, servico, filtro_servico, dimensao)
    st.plotly_chart(fig, use_container_width=False)


@st.cache_data
def get_fig(df, tipo_grafico, servico, filtro_servico, dimensao):
    match tipo_grafico:
        case "Linha":
            fig = px.line(
                df,
                x="Ano",
                y="Nível de acesso em %",
                color=dimensao,
                markers=True,
                title=f"{servico} {filtro_servico.lower()} por {dimensao.lower()}"
            )
        case "Mapa":
            if dimensao != "Município":
                df[dimensao] = df[dimensao].str.upper()
            geo_dict = {
                "Sigla da Unidade da Federação": ["uf", "UF_05"],
                "Mesorregião": ["mesorregiao", "MESO"],
                "Microrregião": ["microrregiao", "MICRO"],
                "Município": ["municipio", "NOME"]
            }

            with open(f"data/geo/{geo_dict[dimensao][0]}.json", encoding="latin1") as f:
                geojson = json.load(f)

            fig = px.choropleth_mapbox(
                df,
                geojson=geojson,
                color="Nível de acesso em %",
                locations=dimensao,
                range_color=(0, 100),
                mapbox_style="white-bg",
                featureidkey=f"properties.{geo_dict[dimensao][1]}",
                center={"lat": -14, "lon": -55},
                animation_frame="Ano",
                color_continuous_scale="Viridis",
                zoom=3,
                width=750,
                height=750,
                title=f"{servico} {filtro_servico.lower()} por {dimensao.lower()}"
            )

        case _:
            fig = None
    return fig


def download(df: pd.DataFrame, tipo_grafico, servico, filtro_servico, dimensao) -> None:
    csv = convert_df(df)
    st.download_button(
        label="Download CSV",
        data=csv,
        file_name=f"{servico} {filtro_servico} por {dimensao}.csv".lower(),
        mime="text/csv",
    )


def main() -> None:
    st.markdown("# Censo escolar")
    tipo_grafico = st.sidebar.selectbox(
        "Tipo de gráfico",
        ["Linha", "Mapa"]
    )
    servico = st.sidebar.selectbox(
        "Serviço",
        [
            "Abastecimento de água",
            "Abastecimento de energia elétrica",
            "Esgoto sanitário",
            "Destinação do lixo",
            "Acesso a internet"]
    )
    match tipo_grafico:
        case "Linha":
            opcoes_dimensao = [
                "Região Geográfica",
                "Sigla da Unidade da Federação",
                "Mesorregião",
                "Microrregião",
                "Município",
                "Dependência Administrativa",
                "Categoria de escola",
                "Localização",
                "Localização diferenciada da escola",
            ]
        case "Mapa":
            opcoes_dimensao = ["Sigla da Unidade da Federação", "Mesorregião", "Microrregião", "Município"]

    dimensao = st.sidebar.selectbox(
        "Dimensão",
        opcoes_dimensao
    )

    df = get_df(servico, dimensao)
    filtro_servico = st.sidebar.selectbox(
        "Filtro serviço",
        df["Serviço"].unique()
    )
    df = get_df_filtrado(df, filtro_servico)

    if flag_executar := st.sidebar.button("Executar"):
        plot(df, tipo_grafico, servico, filtro_servico, dimensao)
        download(df, tipo_grafico, servico, filtro_servico, dimensao)


if __name__ == "__main__":
    main()
