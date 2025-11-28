# redskins_dashboard/jobs/job3_export_to_sharepoint.py

import os
import requests

from redskins_dashboard.sp_client import (
    get_app_token,
    get_site_id,
    SP_HOST,
    SITE_PATH,
)

# Base dir = stripe_test/redskins_dashboard
BASE_DIR = os.path.dirname(os.path.dirname(__file__))

DATA_DIR      = os.path.join(BASE_DIR, "data")
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")
RAW_DIR       = os.path.join(DATA_DIR, "raw")   # <-- separate raw dir

GRAPH = "https://graph.microsoft.com/v1.0"


import os
from datetime import datetime

LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

def write_execution_log(message: str):
    """Append a log entry to job3 monthly log file."""
    month_str = datetime.now().strftime("%Y-%m")
    log_file = os.path.join(LOG_DIR, f"job3_{month_str}.log")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")



def upload_file_to_sharepoint(
    site_id: str,
    token: str,
    local_path: str,
    remote_folder: str,
    content_type: str = "text/csv",
) -> None:
    """
    Uploads a local file to SharePoint using the site's default drive.

    remote_folder: path like "/Shared Documents/redskins_dashboard_processed"
    """
    if not os.path.exists(local_path):
        raise FileNotFoundError(local_path)

    filename = os.path.basename(local_path)

    # Normalize remote folder path
    folder = remote_folder.strip()
    if not folder.startswith("/"):
        folder = "/" + folder

    # Example:
    #   /sites/{site_id}/drive/root:/Shared Documents/redskins_dashboard_processed/cobros_view.csv:/content
    upload_url = (
        f"{GRAPH}/sites/{site_id}/drive/root:"
        f"{folder}/{filename}:/content"
    )

    # Human-friendly URL
    sp_web_url = (
        f"https://{SP_HOST}/sites{SITE_PATH}"
        f"{folder}/{filename}"
    )
    print(f"SharePoint URL: {sp_web_url}")
    print(f"Uploading {filename} -> {folder} ...")

    with open(local_path, "rb") as f:
        data = f.read()

    resp = requests.put(
        upload_url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": content_type,
        },
        data=data,
    )

    if resp.status_code >= 400:
        raise RuntimeError(
            f"Upload failed for {filename}: {resp.status_code} {resp.reason}\n{resp.text}"
        )

    print(f"  ✔ Uploaded {filename}")


def main():
    token = get_app_token()
    site_id = get_site_id(SP_HOST, SITE_PATH, token)

    FILES_TO_UPLOAD = [
        # processed
        ("cobros_view.csv",           "/Shared Documents/redskins_dashboard_processed"),
        ("creditos_view.csv",         "/Shared Documents/redskins_dashboard_processed"),
        ("estado_general_view.csv",   "/Shared Documents/redskins_dashboard_processed"),
        ("creditos_resumen_view.csv", "/Shared Documents/redskins_dashboard_processed"),
        ("jugadores_view.csv",        "/Shared Documents/redskins_dashboard_processed"),
        ("categorias_view.csv",       "/Shared Documents/redskins_dashboard_processed"),

        # raw
        ("jugadores_raw.csv",  "/Shared Documents/redskins_dashboard_raw"),
        ("categorias_raw.csv", "/Shared Documents/redskins_dashboard_raw"),
    ]

    uploaded_files = []
    
    try:
        for filename, remote_folder in FILES_TO_UPLOAD:
            # If it's a “view” file, look in processed; otherwise in raw
            base_dir = PROCESSED_DIR if "view" in filename else RAW_DIR
            local_path = os.path.join(base_dir, filename)

            upload_file_to_sharepoint(site_id, token, local_path, remote_folder)
            uploaded_files.append(filename)

        # Log success after ALL files were uploaded
        write_execution_log(f"SUCCESS — uploaded {len(uploaded_files)} files: {uploaded_files}")

    except Exception as e:
        # Log the failure with the error message
        write_execution_log(f"ERROR — {str(e)}")
        raise



if __name__ == "__main__":
    main()
