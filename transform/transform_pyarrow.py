import duckdb
import pyarrow as pa
from pyarrow import (
    csv,
    dataset as ds
)

table = csv.read_csv(
    'data/2021/dados/microdados_ed_basica_2021.csv',
    read_options=csv.ReadOptions(encoding="latin1"),
    parse_options=csv.ParseOptions(delimiter=";")
)

# connect to an in-memory database
con = duckdb.connect()

print(con.execute("SELECT * FROM table LIMIT 2").fetchone())
# table = duckdb.arrow(table)
#
# table.query(
#     "SELECT * FROM table LIMIT 1;"
# ).fetchone()



