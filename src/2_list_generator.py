import logging
import os

import pandas as pd
from google.cloud import storage

# --------------------------------------------------
# --- SCRIPT CONFIGURATION ---
# --------------------------------------------------

# Name of your Google Cloud Storage bucket.
GCS_BUCKET_NAME = "drycap-tiles-colombia"  # IMPORTANT: Must match your bucket

# The "version suffix" used in the Auditor script run.
# This must match the suffix of the results you want to analyze.
# STAGE 1 -> "q65_audit_dynamic"
# STAGE 2 -> "q50_audit" (or whatever you name it)
VERSION_SUFFIX = "q65_audit_dynamic"

# --- Leave the rest of the script as is ---

# Define file paths based on the configuration
GCS_FOLDER_PATH = f"auditor_results/{VERSION_SUFFIX}"
GCS_FILE_NAME = f"coverage_audit_{VERSION_SUFFIX}.csv"
GCS_BLOB_PATH = f"{GCS_FOLDER_PATH}/{GCS_FILE_NAME}"

LOCAL_CSV_PATH = f"./{GCS_FILE_NAME}"

# Define output file names
PASSED_LIST_FILE = f"passed_{VERSION_SUFFIX}.txt"
FAILED_LIST_FILE = f"failed_{VERSION_SUFFIX}.txt"

# --------------------------------------------------
# --- Main Logic
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a file from the GCS bucket."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)

        logging.info(
            f"Downloading gs://{bucket_name}/{source_blob_name} to {destination_file_name}..."
        )
        blob.download_to_filename(destination_file_name)
        logging.info("Download complete.")
        return True
    except Exception as e:
        logging.error(
            "Failed to download file. Please check bucket/file names and your authentication."
        )
        logging.error(f"Error: {e}")
        return False


def analyze_audit_results():
    """Reads the downloaded CSV and creates passed/failed lists."""
    if not os.path.exists(LOCAL_CSV_PATH):
        logging.error(f"Local file not found: {LOCAL_CSV_PATH}. Cannot analyze.")
        return

    logging.info(f"Reading audit results from {LOCAL_CSV_PATH}...")
    df = pd.read_csv(LOCAL_CSV_PATH)

    # Ensure the 'passes_check' column is treated as boolean
    # GEE exports booleans as 1 (True) and 0 (False) in CSVs
    if "passes_check" not in df.columns:
        logging.error(
            "CSV file is missing the required 'passes_check' column. Aborting."
        )
        return

    # The .geo column can be dropped if it exists
    if ".geo" in df.columns:
        df = df.drop(columns=[".geo"])

    df["passes_check"] = df["passes_check"].astype(bool)

    passed_df = df[df["passes_check"] == True]
    failed_df = df[df["passes_check"] == False]

    logging.info(
        f"Analysis complete: {len(passed_df)} municipalities PASSED, {len(failed_df)} FAILED."
    )

    # --- Write Passed List ---
    passed_ids = passed_df["municipality_id"].astype(int).tolist()
    with open(PASSED_LIST_FILE, "w") as f:
        for mpio_id in passed_ids:
            f.write(f"{mpio_id}\n")
    logging.info(f"Successfully wrote {len(passed_ids)} IDs to {PASSED_LIST_FILE}")

    # --- Write Failed List ---
    failed_ids = failed_df["municipality_id"].astype(int).tolist()
    with open(FAILED_LIST_FILE, "w") as f:
        for mpio_id in failed_ids:
            f.write(f"{mpio_id}\n")
    logging.info(f"Successfully wrote {len(failed_ids)} IDs to {FAILED_LIST_FILE}")


if __name__ == "__main__":
    if download_blob(GCS_BUCKET_NAME, GCS_BLOB_PATH, LOCAL_CSV_PATH):
        analyze_audit_results()
