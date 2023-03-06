import streamlit as st
import plotly.express as px

from utils import DIMENSIONS, run_query, convert_df


def main() -> None:
    st.markdown("# Censo escolar")
    metric = "Quantidade de matrículas"
    dimension = st.sidebar.selectbox("Dimensão", DIMENSIONS.keys())

    st.markdown(f"{metric.upper()} x {dimension.upper()}")
    query = f"""
                select
                    {DIMENSIONS[dimension]} as '{dimension}',
                    cast(sum(QT_MAT_FUND) as bigint) as 'Educação Infantil',
                    cast(sum(QT_MAT_MED) as bigint) as 'Ensino Fundamental',
                    cast(sum(QT_MAT_PROF) as bigint) as 'Ensino Médio',
                    cast(sum(QT_MAT_PROF_TEC) as bigint) as 'Educação Profissional',
                    cast(sum(QT_MAT_EJA) as bigint) as 'Educação de Jovens e Adultos (EJA)',
                    cast(sum(QT_MAT_ESP) as bigint) as 'Educação Especial'
                from microdados
                group by {DIMENSIONS[dimension]}
            """
    df = run_query(query)
    df = df.melt(
        id_vars=[dimension],
        var_name="Nível de ensino",
        value_name="Quantidade de matrículas",
        value_vars=df.columns[1:]
     )
    fig = px.bar(
        df,
        x=dimension,
        y="Quantidade de matrículas",
        color="Nível de ensino"
    )
    st.plotly_chart(fig, use_container_width=True)
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
