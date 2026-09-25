"""
make_charts.py
---------------
Queries the SQL views (built on the clean data in DuckDB) and produces
a couple of simple charts with matplotlib. Run this after the pipeline
(run_pipeline.py) has populated the DuckDB warehouse.

Run from the project root:
    python3 src/make_charts.py
"""

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt

WAREHOUSE_PATH = "data/warehouse/ed.duckdb"
VIEWS_SQL_PATH = "sql/views.sql"
CHARTS_DIR = Path("reports/charts")


def get_connection():
    con = duckdb.connect(WAREHOUSE_PATH)
    with open(VIEWS_SQL_PATH) as f:
        con.execute(f.read())
    return con


def chart_arrivals_by_hour(con):
    df = con.execute("SELECT * FROM v_arrivals_by_hour ORDER BY arrival_hour").fetchdf()

    fig, ax = plt.subplots(figsize=(9, 4.5))
    ax.bar(df["arrival_hour"], df["attendances"], color="#2E7D9A")
    ax.set_xlabel("Hour of arrival")
    ax.set_ylabel("Number of attendances")
    ax.set_title("ED Arrivals by Hour of Day")
    ax.set_xticks(range(0, 24))
    fig.tight_layout()

    out_path = CHARTS_DIR / "arrivals_by_hour.png"
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path


def chart_los_by_triage(con):
    df = con.execute("SELECT * FROM v_los_by_triage ORDER BY triage_category").fetchdf()

    fig, ax = plt.subplots(figsize=(7, 4.5))
    ax.bar(df["triage_category"], df["median_los_minutes"], color="#C0392B")
    ax.set_xlabel("Triage category (1 = most urgent)")
    ax.set_ylabel("Median length of stay (minutes)")
    ax.set_title("Median Length of Stay by Triage Category")
    fig.tight_layout()

    out_path = CHARTS_DIR / "los_by_triage.png"
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path


if __name__ == "__main__":
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    con = get_connection()

    breach = con.execute("SELECT * FROM v_four_hour_breach").fetchdf()
    print("4-hour breach rate:")
    print(breach.to_string(index=False))

    path1 = chart_arrivals_by_hour(con)
    print(f"\nSaved: {path1}")

    path2 = chart_los_by_triage(con)
    print(f"Saved: {path2}")

    con.close()