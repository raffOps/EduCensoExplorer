import json

import pandas as pd
import plotly.express as px
import streamlit as st
from utils import INDICADORES, run_query, convert_df

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

    "Esforço Docente":
        "Esforço docente mensura o esforço empreendido pelos docentes da educação básica brasileira no exercício de sua profissão.\n"
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

DIMENSOES_GEOGRAFICAS = {
    "Unidade da Federação": ["SG_UF", "uf", "UF_05"],
    "Município": ["NO_MUNICIPIO", "municipio", "NOME"]
}


@st.cache_data
def get_valores_possiveis(indicador: str, coluna: str) -> list[str]:
    query = f"select distinct({coluna}) as value from {INDICADORES[indicador]} order by 1"
    return run_query(query)["value"].tolist()


def get_df_linha(
        indicador: str,
        localização: str,
        dependencia: str,
        label_dimensao_geografica: str,
        dimensao_geografica: str
) -> pd.DataFrame:
    query = f"""
                select
                    NU_ANO_CENSO as 'Ano',
                    TP_GRUPO as 'Grupo',
                    avg(METRICA) AS '{indicador}'
                from {INDICADORES[indicador]} i
                where
                    NO_CATEGORIA='{localização}'
                    and NO_DEPENDENCIA='{dependencia}'
                    and {DIMENSOES_GEOGRAFICAS[label_dimensao_geografica][0]}='{dimensao_geografica}'
                group by NU_ANO_CENSO, TP_GRUPO
                order by 1, 2
            """
    return run_query(query)


def get_df_mapa(
        indicador: str,
        localização: str,
        dependencia: str,
        dimensao_geografica: str,
        grupo: str
) -> pd.DataFrame:
    query = f"""
                select
                    NU_ANO_CENSO as 'Ano',
                    {DIMENSOES_GEOGRAFICAS[dimensao_geografica][0]} as '{dimensao_geografica}',
                    avg(METRICA) AS '{indicador}'
                from {INDICADORES[indicador]} i
                where
                    NO_CATEGORIA='{localização}'
                    and NO_DEPENDENCIA='{dependencia}'
                    and TP_GRUPO='{grupo}'
                group by NU_ANO_CENSO, {DIMENSOES_GEOGRAFICAS[dimensao_geografica][0]}
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
def plot_mapa(df: str, indicador: str, label_dimensao_geografica: str, title: str) -> None:
    with open(f"data/geo/{DIMENSOES_GEOGRAFICAS[label_dimensao_geografica][1]}.json", encoding="latin1") as f:
        geojson = json.load(f)
    fig = px.choropleth_mapbox(
        df,
        geojson=geojson,
        color=indicador,
        locations=label_dimensao_geografica,
        range_color=(0, 100),
        mapbox_style="white-bg",
        featureidkey=f"properties.{DIMENSOES_GEOGRAFICAS[label_dimensao_geografica][2]}",
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


def linha(localização: str, dependencia: str, indicador: str, label_dimensao_geografica: str) -> None:
    filtro_dimensao_geografica = st.sidebar.selectbox(
        "Filtro dimensão geográfica",
        get_valores_possiveis(indicador, DIMENSOES_GEOGRAFICAS[label_dimensao_geografica][0])
    )
    grupo = st.sidebar.multiselect(
        "Grupo",
        get_valores_possiveis(indicador, "TP_GRUPO")
    )
    df = get_df_linha(indicador, localização, dependencia, label_dimensao_geografica,
                      filtro_dimensao_geografica)
    df = get_df_filtrado(df, "Grupo", grupo)
    title = f"{indicador} | {label_dimensao_geografica} - {filtro_dimensao_geografica} " \
            f"| Localização {localização} | Dependência {dependencia}"
    plot_linha(df, indicador, title)
    download(df, title)


def mapa(
        localização: str,
        dependencia: str,
        indicador: str,
        label_dimensao_geografica: str
) -> None:
    grupo = st.sidebar.selectbox(
        "Grupo",
        get_valores_possiveis(indicador, "TP_GRUPO")
    )

    df = get_df_mapa(indicador, localização, dependencia, label_dimensao_geografica, grupo)
    title = title = f"{indicador} | Grupo - {grupo} " \
                    f"| Localização {localização} | Dependência {dependencia}"
    if st.button("Executar"):
        plot_mapa(df, indicador, label_dimensao_geografica, title)
        download(df, title)


def main() -> None:
    tipo_grafico = st.sidebar.selectbox(
        "Tipo de gráfico",
        ["Linha", "Mapa"]
    )
    indicador = st.sidebar.selectbox(
        "Indicador",
        INDICADORES.keys()
    )
    localização = st.sidebar.selectbox(
        "Localização",
        ["Rural", "Urbana"]
    )
    dependencia = st.sidebar.selectbox(
        "Dependência",
        ['Estadual', 'Municipal', 'Privada', 'Federal']
    )
    label_dimensao_geografica = st.sidebar.selectbox(
        "Dimensão geográfica",
        DIMENSOES_GEOGRAFICAS.keys()
    )

    if tipo_grafico == "Linha":
        linha(localização, dependencia, indicador, label_dimensao_geografica)
    else:
        mapa(localização, dependencia, indicador, label_dimensao_geografica)

    if INDICADORES[indicador] in ("AFD", "IED", "TDI"):
        st.markdown(f"{WIKI_INDICADORES[indicador]}")


if __name__ == "__main__":
    main()
