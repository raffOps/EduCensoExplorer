import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)

INDICADORES = {
    #"AFD": "Adequação da Formação Docente",
    #"ICG": "Complexidade de Gestão da Escola",
    #"IED": "Esforço Docente",
    #"ATU": "Média de Alunos por Turma",
    "HAD": "Média de Horas-aula diária",
    "DSU": "Percentual de Docentes com Curso Superior",
    "TDI": "Taxas de Distorção Idade-série"
}


def get_dataframe(indicador, year):
    file = f"./data/raw/{indicador}/{indicador}_{year}_ESCOLAS/{indicador}_ESCOLAS_{year}.xlsx"
    try:
        match indicador:
            case "ATU" | "HAD" | "DSU":
                df = pd.read_excel(file, skiprows=8, skipfooter=6, na_values=["--"])
                df["CO_ENTIDADE"], df["NO_ENTIDADE"] = df["NO_ENTIDADE"], df["CO_ENTIDADE"]
                df["CO_MUNICIPIO"], df["NO_MUNICIPIO"] = df["NO_MUNICIPIO"], df["CO_MUNICIPIO"]
            case _:
                print("aqui")
                df = pd.read_excel(file, skiprows=10, skipfooter=6, na_values=["--"])
        return df
    except FileNotFoundError:
        raise FileNotFoundError(file)


def transform_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # rename columns
    df.rename(
        columns={
            "Ano": "NU_ANO_CENSO",
            "SIGLA": "SG_UF",
            "PK_COD_MUNICIPIO": "CO_MUNICIPIO",
            "PK_COD_ENTIDADE": "CO_ENTIDADE",
            "TIPOLOCA": "NO_CATEGORIA",
            "Dependad": "NO_DEPENDENCIA"
        },
        inplace=True
    )

    # melt
    id_vars = df.columns[:9]
    value_vars = df.columns[9:]
    df = df.melt(id_vars=id_vars, value_vars=value_vars, var_name="GRUPO", value_name="METRICA")

    # type
    df["NU_ANO_CENSO"] = df["NU_ANO_CENSO"].astype("int32")
    df[["CO_MUNICIPIO", "CO_ENTIDADE"]] = df[["CO_MUNICIPIO", "CO_ENTIDADE"]].astype("int64")

    df = df.drop(columns=["NO_ENTIDADE"])
    return df


def save_to_parquet(df: pd.DataFrame, indicador) -> None:
    df.to_parquet(
        f"./data/transformed/indicadores/{indicador}.parquet",
        engine="pyarrow",
        compression="snappy",
        index=None,
        partition_cols=["NU_ANO_CENSO"]
    )


def main():
    logger = logging.getLogger(name="indicadores - transform")
    for indicador in INDICADORES:
        for year in range(2016, 2022):
            logger.info(f"{indicador} - {year}")
            df = get_dataframe(indicador, year)
            df = transform_dataframe(df)
            save_to_parquet(df, indicador)


if __name__ == "__main__":
    main()
