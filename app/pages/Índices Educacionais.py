import json

import pandas as pd
import plotly.express as px
import streamlit as st
from utils import INDICADORES, run_query, convert_df, get_valores_possiveis


DIMENSOES_GEOGRAFICAS = {
    "País",
    "Unidade Federativa",
    "Município"
}

WIKI_INDICADORES = {
    "Adequação da Formação Docente": "Adequação da formação docente é um indicador da adequação da formação inicial dos "
                                     "docentes das escolas de educação básica brasileira, segundo as orientações legais.\n\n"
                                     " - Grupo 1: Docentes com formação superior de licenciatura na mesma disciplina "
                                     "que lecionam, ou bacharelado na mesma disciplina com curso de complementação "
                                     "pedagógica concluído.\n\n"
                                     " - Grupo 2: Docentes com formação superior de bacharelado na disciplina "
                                     "correspondente, mas sem licenciatura ou complementação pedagógica.\n\n"
                                     " - Grupo 3: Docentes com licenciatura em área diferente daquela que leciona, "
                                     "ou combacharelado nas disciplinas da base curricular comum e complementação "
                                     "pedagógica concluída em área diferente daquela que leciona.\n\n"
                                     "- Grupo 4: Docentes com outra formação superior não considerada nas localizaçãos anteriores.\n"
                                     "- Grupo 5: Docentes que não possuem curso superior completo\n\n"
                                     "[Fonte](https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2014/docente_formacao_legal/nota_tecnica_indicador_docente_formacao_legal.pdf)",

    "Índice de Esforço Docente":
        "Índice de Esforço Docente mensura o esforço empreendido pelos docentes da educação básica brasileira no exercício de sua profissão.\n"
        "- Nível 1: Docente que tem até 25 alunos e atua em um único turno, escola e etapa.\n\n"
        "- Nível 2: Docente que tem entre 25 e 150 alunos e atua em um único turno, escola e etapa.\n\n"
        "- Nível 3: Docente que tem entre 25 e 300 alunos e atua em um ou dois turnos em uma única escola e etapa.\n\n"
        "- Nível 4: Docentes que tem entre 50 e 400 alunos e atua em dois turnos, em uma ou duas escolas e em duas etapas.\n\n"
        "- Nível 5: Docente que tem mais de 300 alunos e atua nos três turnos, em duas ou três escolas "
        "e em duas etapas ou três etapas.\n\n"
        "- Nível 6: Docente que tem mais de 400 alunos e atua nos três turnos, em duas ou três escolas "
        "e em duas etapas ou três etapas.\n\n"
        "[Fonte](https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2014/docente_esforco/nota_tecnica_indicador_docente_esforco.pdf)",
    "Taxas de Distorção Idade-série":
        "Taxas de distorção idade-série representa a proporção de alunos com mais de 2 anos de atraso escolar.\n\n"
        "[Fonte](https://www.sed.sc.gov.br/legislacoes-estadual-e-federal/censo-278/indicadores-disponibilizados-pelo-inep/distorcao-serie-idade/12280-nota-tecnica-distorcao-idade-serie/file)"
}


def get_df_linha(
        indicador: str,
        Localidade: str,
        dependencia: str,
        localidade_geografica: str,
) -> pd.DataFrame:
    query = f"""
                select
                    NU_ANO_CENSO as 'Ano',
                    TP_GRUPO as 'Grupo',
                    METRICA AS '{indicador}'
                from {INDICADORES[indicador]} i
                where
                    NO_LOCALIDADE_GEOGRAFICA='{localidade_geografica}'
                    and NO_CATEGORIA = '{Localidade}'
                    and NO_DEPENDENCIA = '{dependencia}'
            """
    return run_query(query)


def get_df_mapa(
        indicador: str,
        localidade: str,
        dependencia: str,
        dimensao_geografica: str,
        grupo: str,
) -> pd.DataFrame:
    query = f"""
                select
                    NU_ANO_CENSO as 'Ano',
                    NO_LOCALIDADE_GEOGRAFICA as '{dimensao_geografica}',
                    METRICA AS '{indicador} | {grupo}'
                from {INDICADORES[indicador]} i
                where
                    TP_GRUPO='{grupo}'
                    and TP_LOCALIDADE_GEOGRAFICA='{dimensao_geografica}'
                    and NO_CATEGORIA = '{localidade}'
                    and NO_DEPENDENCIA = '{dependencia}'
            """
    return run_query(query)


# @st.cache_data
def get_df_filtrado(df: pd.DataFrame, coluna: str, filtro: list[str]) -> pd.DataFrame:
    if filtro:
        df = df[df[coluna].isin(filtro)]
    return df


def plot_linha(
        df: pd.DataFrame,
        indicador: str,
        title: str
) -> None:
    fig = px.line(
        df,
        x="Ano",
        y=indicador,
        color="Grupo",
        markers=True,
        title=title
    )
    st.plotly_chart(fig, use_container_width=True)


@st.cache_data
def plot_mapa(df: str, indicador: str,  title: str) -> None:
    locations = df.columns[1]
    if locations == "Município":
        geojson_file = "data/geo/municipio.json"
        geokey = "NOME"
    else:
        geojson_file = "data/geo/uf.json"
        geokey = "NOME_UF"

    with open(geojson_file, encoding="latin1") as f:
        geojson = json.load(f)

    fig = px.choropleth_mapbox(
        df,
        geojson=geojson,
        color=df.columns[2],
        locations=locations,
        mapbox_style="white-bg",
        featureidkey=f"properties.{geokey}",
        center={"lat": -14, "lon": -55},
        animation_frame="Ano",
        color_continuous_scale="Viridis",
        zoom=3,
        width=750,
        height=750,
        title=title,
    )
    st.plotly_chart(fig, use_container_width=True)


def download(df: pd.DataFrame,
             title: str
             ) -> None:
    csv = convert_df(df)
    st.download_button(
        label="Download CSV",
        data=csv,
        file_name=f"{title}.csv".lower(),
        mime="text/csv",
    )


def linha(Localidade: str, dependencia: str, indicador: str, dimensao_geografica: str) -> None:
    filtro_dimensao_geografica = st.sidebar.selectbox(
        "Filtro dimensão geográfica",
        run_query(f"select distinct NO_LOCALIDADE_GEOGRAFICA from {INDICADORES[indicador]} "
                  f"where TP_LOCALIDADE_GEOGRAFICA='{dimensao_geografica}'")
    )
    df = get_df_linha(
        indicador,
        Localidade,
        dependencia,
        localidade_geografica=filtro_dimensao_geografica
    )
    grupo = st.sidebar.multiselect(
        "Grupo",
        get_valores_possiveis(indicador, "TP_GRUPO")
    )
    df = get_df_filtrado(df, "Grupo", grupo)
    title = f"{indicador} | {dimensao_geografica} - {filtro_dimensao_geografica} " \
            f"| Localidade {Localidade} | Dependência {dependencia}"
    plot_linha(df, indicador, title)
    download(df, title)


def mapa(
        Localidade: str,
        dependencia: str,
        indicador: str,
        dimensao_geografica: str
) -> None:
    grupo = st.sidebar.selectbox(
        "Grupo",
        get_valores_possiveis(indicador, "TP_GRUPO")
    )

    df = get_df_mapa(
        indicador,
        Localidade,
        dependencia,
        dimensao_geografica,
        grupo=grupo
    )
    title = title = f"{indicador} | Grupo - {grupo} " \
                    f"| Localidade {Localidade} | Dependência {dependencia}"
    if st.button("Executar"):
        plot_mapa(df, indicador, title)
        download(df, title)


def main() -> None:
    tipo_grafico = st.sidebar.selectbox(
        "Tipo de gráfico",
        ["Mapa", "Linha"]
    )
    indicador = st.sidebar.selectbox(
        "Indicador",
        INDICADORES.keys()
    )
    if tipo_grafico == "linha":
        dimensao_geografica = st.sidebar.selectbox(
            "Dimensão geográfica",
            DIMENSOES_GEOGRAFICAS.keys()
        )
    else:
        dimensao_geografica = st.sidebar.selectbox(
            "Dimensão geográfica",
            ["Unidade Federativa", "Município"]
        )

    Localidade = st.sidebar.selectbox(
        "Localidade",
        ["Total", "Rural", "Urbana"]
    )
    dependencia = st.sidebar.selectbox(
        "Dependência",
        ["Total", "Estadual", "Municipal", "Privada", "Federal"]
    )
    if tipo_grafico == "Linha":
        linha(Localidade, dependencia, indicador, dimensao_geografica)
    else:
        mapa(Localidade, dependencia, indicador, dimensao_geografica)

    if INDICADORES[indicador] in ("AFD", "IED", "TDI"):
        st.markdown(f"{WIKI_INDICADORES[indicador]}")


if __name__ == "__main__":
    main()
