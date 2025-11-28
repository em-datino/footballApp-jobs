# job1_ingest_from_sharepoint.py

import os
import pandas as pd
from redskins_dashboard.sp_client import (
    get_app_token,
    get_site_id,
    get_list_id,
    read_list,
    SP_HOST,
    SITE_PATH,
)

# Directory where raw snapshots will be stored
BASE_DIR = os.path.dirname(os.path.dirname(__file__))  # stripe_test/
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")

os.makedirs(RAW_DIR, exist_ok=True)

# SharePoint list display names (as they appear in SharePoint)
JUGADORES_LIST_NAME  = "Jugadores"
COBROS_LIST_NAME     = "Cobros"
CATEGORIAS_LIST_NAME = "Categorias"
CREDITOS_LIST_NAME   = "Creditos"


def dump_list_to_csv(site_id: str, token: str, list_display_name: str,
                     output_filename: str):
    """
    Reads a SharePoint list (all columns) and writes it to a CSV in RAW_DIR.
    """
    print(f"Reading list '{list_display_name}'...")
    list_id = get_list_id(site_id, list_display_name, token)
    df = read_list(site_id, list_id, token)  # <-- ALL columns returned by Graph
    output_path = os.path.join(RAW_DIR, output_filename)
    df.to_csv(output_path, index=False)
    print(f"  -> {output_path} ({len(df)} rows)")


def main():
    # 1) Auth & site
    token   = get_app_token()
    site_id = get_site_id(SP_HOST, SITE_PATH, token)

    # 2) Lists -> raw CSVs
    dump_list_to_csv(site_id, token, JUGADORES_LIST_NAME,  "jugadores_raw.csv")
    dump_list_to_csv(site_id, token, COBROS_LIST_NAME,     "cobros_raw.csv")
    dump_list_to_csv(site_id, token, CATEGORIAS_LIST_NAME, "categorias_raw.csv")
    dump_list_to_csv(site_id, token, CREDITOS_LIST_NAME,   "creditos_raw.csv")

    print("Job1 complete: all raw lists exported to data/raw/.")


if __name__ == "__main__":
    main()
