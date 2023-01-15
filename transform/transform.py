import duckdb
import pandas as pd

df = pd.read_csv('data/2021/dados/microdados_ed_basica_2021.csv', delimiter=";", encoding="latin1")

con = duckdb.connect(database="data/my-db.duckdb", read_only=False)

con.execute(
    "SELECT * FROM df"
)
print(con.fetchone())