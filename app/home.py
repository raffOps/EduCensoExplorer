import streamlit as st

from utils import DIMENSIONS, run_query, convert_df

metrics = [
    "Quantidade de matrículas",
    "Quantidade de escolas"
]


def main() -> None:
    st.markdown("# Censo escolar")

    metric = st.sidebar.selectbox("Métrica", metrics)
    dimension = st.sidebar.selectbox("Dimensão", DIMENSIONS.keys())

    st.markdown(f"{metric.upper()} x {dimension.upper()}")

    match metric:
        case "Quantidade de matrículas":
            query = f"""
                        select
                            {DIMENSIONS[dimension]} as '{dimension}',
                            cast(sum(QT_MAT_INF) as bigint) as 'Educação Básica',
                            cast(sum(QT_MAT_FUND) as bigint) as 'Educação Infantil',
                            cast(sum(QT_MAT_MED) as bigint) as 'Ensino Fundamental',
                            cast(sum(QT_MAT_PROF) as bigint) as 'Ensino Médio',
                            cast(sum(QT_MAT_PROF_TEC) as bigint) as 'Educação Profissional',
                            cast(sum(QT_MAT_EJA) as bigint) as 'Educação de Jovens e Adultos (EJA)',
                            cast(sum(QT_MAT_ESP) as bigint) as 'Educação Especial'
                        from microdados
                        group by {dimension}
                    """
        case "Quantidade de escolas":
            query = f"""
                    select
                        {DIMENSIONS[dimension]} as '{dimension}',
                        count(distinct(CO_ENTIDADE)) as 'Quantidade de escolas'
                    from censo
                    group by {DIMENSIONS[dimension]}
                    order by 1
                    """
        case _:
            query = None

    df = run_query(query)
    st.bar_chart(df, x=dimension, y=df.columns[1:], use_container_width=True)
    st.dataframe(df)
    csv = convert_df(df)
    st.download_button(
        label="Download",
        data=csv,
        file_name=f"{metric}_{dimension}.csv",
        mime="text/csv",
    )


if __name__ == "__main__":
    main()
