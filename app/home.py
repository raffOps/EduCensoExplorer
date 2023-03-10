import streamlit as st

from utils import DIMENSIONS, run_query, convert_df

metrics = [
    "Quantidade de matrículas",
    "Quantidade de escolas"
]


def main() -> None:
    st.markdown("# Censo escolar")


if __name__ == "__main__":
    main()
