"""
load.py
-------
Writes the clean DataFrame into a DuckDB database file, so it can be
queried with plain SQL afterwards (see sql/views.sql).
"""

import duckdb

WAREHOUSE_PATH = "data/warehouse/ed.duckdb"


def load_clean_to_duckdb(clean_pdf, warehouse_path: str = WAREHOUSE_PATH):
    """clean_pdf is a pandas DataFrame (converted from Spark) — small
    enough at this point (19,150 rows) that pandas is the right tool,
    per the pattern discussed earlier: Spark for the heavy cleaning,
    pandas for the small stuff downstream."""
    con = duckdb.connect(warehouse_path)
    con.execute("CREATE OR REPLACE TABLE ed_attendances_clean AS SELECT * FROM clean_pdf")
    row_count = con.execute("SELECT COUNT(*) FROM ed_attendances_clean").fetchone()[0]
    con.close()
    return row_count
