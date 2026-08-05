"""One-time data prep: unzip the UCI archive and convert the xlsx to CSV.

Run once on the host (or in the container) before the producer.
Source: https://archive.ics.uci.edu/dataset/352/online+retail (CC BY 4.0).
"""
import os
import zipfile

import pandas as pd

from config import DATA_DIR, RAW_ZIP, RAW_CSV


def main() -> None:
    if os.path.exists(RAW_CSV):
        print(f"already prepared: {RAW_CSV}")
        return
    with zipfile.ZipFile(RAW_ZIP) as zf:
        xlsx_name = next(n for n in zf.namelist() if n.lower().endswith(".xlsx"))
        zf.extract(xlsx_name, DATA_DIR)
    xlsx_path = os.path.join(DATA_DIR, xlsx_name)
    print(f"reading {xlsx_path} (541k rows — takes a few minutes)...")
    df = pd.read_excel(xlsx_path, dtype=str, engine="openpyxl")
    # normalize the InvoiceDate that pandas parsed as datetime back to UCI's text format
    dates = pd.to_datetime(df["InvoiceDate"], errors="coerce")
    df["InvoiceDate"] = (
        dates.dt.month.astype("Int64").astype(str) + "/"
        + dates.dt.day.astype("Int64").astype(str) + "/"
        + dates.dt.year.astype("Int64").astype(str) + " "
        + dates.dt.hour.astype("Int64").astype(str) + ":"
        + dates.dt.minute.astype("Int64").astype(str).str.zfill(2)
    )
    df.to_csv(RAW_CSV, index=False, encoding="utf-8")
    print(f"wrote {len(df):,} rows -> {RAW_CSV}")


if __name__ == "__main__":
    main()
