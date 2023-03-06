import streamlit as st
import plotly.express as px

from utils import DIMENSIONS, run_query, convert_df


def main() -> None:
    st.markdown("# Censo escolar")
    metric = "Quantidade de escolas"
    dimension = st.sidebar.selectbox("Dimensão", DIMENSIONS.keys())

    st.markdown(f"{metric.upper()} x {dimension.upper()}")
    query = f"""
                select
                    {DIMENSIONS[dimension]} as '{dimension}',
                    count(CO_ENTIDADE) as 'Quantidade de escolas'
                from microdados
                group by {DIMENSIONS[dimension]}
            """
    df = run_query(query)
    fig = px.bar(
        df,
        x=dimension,
        y='Quantidade de escolas'
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
