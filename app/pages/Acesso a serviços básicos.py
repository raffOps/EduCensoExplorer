import json

import pandas as pd
import plotly.express as px
import streamlit as st
from utils import DIMENSOES_GEOGRAFICAS, DIMENSOES, run_query, convert_df, get_valores_possiveis


@st.cache_data
def get_df(
        servico: str,
        dimensao_geografica: str,
        label_dimensao_geografica: str,
        filtro_localidade: str,
        filtro_dependencia: str
) -> pd.DataFrame:
    if filtro_localidade == "Total":
        string_filtro_localidade = "1"
    else:
        string_filtro_localidade = f"TP_LOCALIZACAO='{filtro_localidade}'"

    if filtro_dependencia == "Total":
        string_filtro_dependencia = "1"
    else:
        string_filtro_dependencia = f"TP_DEPENDENCIA='{filtro_dependencia}'"

    if dimensao_geografica in {"NO_UF", "NO_MESORREGIAO", "NO_MICRORREGIAO", "NO_MUNICIPIO"}:
        codigo_dimensao_geografica = dimensao_geografica.replace("NO", "CO")
        label_cod_dimensao_geo = f"Código {label_dimensao_geografica}"
        string_cod_dimensao_geo = f"{codigo_dimensao_geografica} as '{label_cod_dimensao_geo}'"
        groupby_cod_dimensao = ", 3"
    else:
        string_cod_dimensao_geo = "0 as dummy"
        groupby_cod_dimensao = ""

    match servico:
        case "Abastecimento de água":
            query = f"""
                select
                    NU_ANO_CENSO as 'Ano',
                    {dimensao_geografica} as '{label_dimensao_geografica}',
                    {string_cod_dimensao_geo},
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
                where {string_filtro_dependencia} and {string_filtro_localidade}
                group by 1, 2 {groupby_cod_dimensao}
                order by 1
            """
        case "Abastecimento de energia elétrica":
            query = f"""
                select
                    NU_ANO_CENSO as 'Ano',
                    {dimensao_geografica} as '{label_dimensao_geografica}',
                    {string_cod_dimensao_geo},
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
                where {string_filtro_dependencia} and {string_filtro_localidade}
                group by 1, 2 {groupby_cod_dimensao}
                order by 1
            """
        case "Esgoto sanitário":
            query = f"""
                select
                    NU_ANO_CENSO as 'Ano',
                    {dimensao_geografica} as '{label_dimensao_geografica}',
                    {string_cod_dimensao_geo},
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
                where {string_filtro_dependencia} and {string_filtro_localidade}
                group by 1, 2 {groupby_cod_dimensao}
                order by 1
            """
        case "Destinação do lixo":
            query = f"""
                select
                    NU_ANO_CENSO as 'Ano',
                    {dimensao_geografica} as '{label_dimensao_geografica}',
                    {string_cod_dimensao_geo},
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
                where {string_filtro_dependencia} and {string_filtro_localidade}
                group by 1, 2 {groupby_cod_dimensao}
                order by 1
            """
        case "Acesso a internet":
            query = f"""
                select
                    NU_ANO_CENSO as 'Ano',
                    {dimensao_geografica} as '{label_dimensao_geografica}',
                    {string_cod_dimensao_geo},
                    round(cast(count(*) filter (where IN_INTERNET) as float) / count(*), 3) as 'Acesso a internet',
                from microdados
                where {string_filtro_dependencia} and {string_filtro_localidade}
                group by 1, 2 {groupby_cod_dimensao}
                order by 1
            """
    df = run_query(query)
    df = df.melt(
        id_vars=df.columns[:3],
        var_name="Serviço",
        value_name="Taxa de acesso",
        value_vars=df.columns[3:]
    )
    df["Nível de acesso em %"] = df["Taxa de acesso"] * 100
    df = df.drop(columns=["Taxa de acesso"])
    return df


@st.cache_data
def get_df_filtrado(df: pd.DataFrame, coluna: str, filtro: str | list[str]) -> pd.DataFrame:
    if filtro:
        if isinstance(filtro, str):
            filtro = [filtro]
        df = df[df[coluna].isin(filtro)]

    return df


def plot_linha(df: pd.DataFrame, title: str) -> None:
    fig = px.line(
        df,
        x="Ano",
        y="Nível de acesso em %",
        color="Serviço",
        markers=True,
        title=title
    )
    st.plotly_chart(fig, use_container_width=True)


# @st.cache_data
def plot_mapa(df: pd.DataFrame, dimensao: str, title: str) -> None:
    if dimensao != "Município":
        df[dimensao] = df[dimensao].str.upper()
    geo_dict = {
        "Unidade da Federação": ["uf", "UF_05"],
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
        locations=df.columns[2],
        featureidkey="properties.GEOCODIGO",
        hover_name=df.columns[1],
        hover_data=df.columns,
        animation_frame="Ano",
        center={"lat": -14, "lon": -55},
        mapbox_style="white-bg",
        color_continuous_scale="Viridis",
        zoom=3,
        width=750,
        height=750,
        title=title
    )

    st.plotly_chart(fig, use_container_width=True)


def download(df: pd.DataFrame, title: str) -> None:
    csv = convert_df(df)
    st.download_button(
        label="Download CSV",
        data=csv,
        file_name=title.lower(),
        mime="text/csv",
    )


def linha(
        label_dimensao_geografica: str,
        filtro_localidade: str,
        filtro_dependencia: str,
        servico: str
) -> None:
    filtro_dimensao_geografica = st.sidebar.selectbox(
        label_dimensao_geografica,
        get_valores_possiveis("microdados", DIMENSOES_GEOGRAFICAS[label_dimensao_geografica])
    )
    df = get_df(
        servico,
        DIMENSOES_GEOGRAFICAS[label_dimensao_geografica],
        label_dimensao_geografica,
        filtro_localidade,
        filtro_dependencia
    )
    df = get_df_filtrado(df, label_dimensao_geografica, filtro_dimensao_geografica)
    title = f"{servico} | {label_dimensao_geografica} - {filtro_dimensao_geografica} | " \
            f"Dependência Administrativa - {filtro_dependencia} | Localidade - {filtro_localidade}"
    plot_linha(df, title)
    download(df, title)


def mapa(
        label_dimensao_geografica: str,
        filtro_localidade: str,
        filtro_dependencia: str,
        servico: str
) -> None:
    df = get_df(
        servico,
        DIMENSOES_GEOGRAFICAS[label_dimensao_geografica],
        label_dimensao_geografica,
        filtro_localidade,
        filtro_dependencia
    )
    filtro_servico = st.sidebar.selectbox(
        servico,
        df["Serviço"].unique().tolist()
    )
    df = get_df_filtrado(df, "Serviço", filtro_servico)
    df = df.drop(columns=["Serviço"])
    title = title = f"{servico} - {filtro_servico}| " \
                    f"Dependência Administrativa - {filtro_dependencia} | Localidade - {filtro_localidade}"
    if st.button("Executar"):
        plot_mapa(df, label_dimensao_geografica, title)
        download(df, title)


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
    if tipo_grafico == "Linha":
        label_dimensao_geografica = st.sidebar.selectbox(
            "Dimensão geográfica",
            [
                "País",
                "Unidade da Federação",
                "Mesorregião",
                "Microrregião",
                "Município",
            ]
        )
    else:
        label_dimensao_geografica = st.sidebar.selectbox(
            "Dimensão geográfica",
            [
                "Unidade da Federação",
                "Mesorregião",
                "Microrregião",
                "Município",
            ]
        )

    filtro_dependencia = st.sidebar.selectbox(
        "Dependência Administrativa",
        ["Total"] + get_valores_possiveis("microdados", DIMENSOES["Dependência Administrativa"])
    )

    filtro_localidade = st.sidebar.selectbox(
        "Localidade",
        ["Total"] + get_valores_possiveis("microdados", DIMENSOES["Localidade"])
    )

    match tipo_grafico:
        case "Linha":
            linha(
                label_dimensao_geografica,
                filtro_localidade,
                filtro_dependencia,
                servico
            )
        case "Mapa":
            mapa(
                label_dimensao_geografica,
                filtro_localidade,
                filtro_dependencia,
                servico
            )


if __name__ == "__main__":
    main()
