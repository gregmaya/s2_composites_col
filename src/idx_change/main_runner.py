import csv
import json
import logging
import os

import index_lib as lib

# Import your runners
from runners import whole

# from runners import urban, rural, roads (Uncomment as you build them)

# --- CONFIG ---
MASTER_JSON = "data/master_composites_index_v3.json"
OUTPUT_DIR = "data/results"  # Dedicated folder for final results

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")


def run_module_full(module, name):
    logging.info(f"--- Starting Production Run: {name} ---")

    # 1. Load Data
    try:
        with open(MASTER_JSON, "r") as f:
            data = json.load(f)
    except FileNotFoundError:
        logging.error("Master JSON not found.")
        return

    # 2. Setup Output File
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filepath = os.path.join(OUTPUT_DIR, f"{name.lower()}_results.csv")

    # 3. check for Resume capability
    processed_ids = set()
    file_exists = os.path.exists(filepath)
    if file_exists:
        with open(filepath, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                processed_ids.add(row["id"])
        logging.info(f"Found {len(processed_ids)} already processed. Resuming...")

    # 4. Open File in Append Mode
    # We use 'a' to append rows as they finish.
    with open(filepath, "a", newline="") as f:
        writer = None

        # If file exists, we need to re-create the writer with fieldnames
        # BUT we don't know fieldnames until we get the first result...
        # Standard trick: buffer the first result if new file.

        for i, entry in enumerate(data):
            mpio_id = entry["id"]

            if mpio_id in processed_ids:
                continue

            if i % 10 == 0:
                logging.info(f"[{name}] Processing {i}/{len(data)}: {mpio_id}")

            try:
                row = module.process(entry)
                if not row:
                    logging.warning(f"[{name}] Empty result for {mpio_id}")
                    continue

                # Init Writer (Lazy Init)
                if writer is None:
                    # Sort keys nicely
                    base = [
                        "id",
                        "type",
                        "quality",
                        "selection_reason",
                        "cov_19",
                        "cov_23",
                    ]
                    # Dynamic stats keys
                    stats_keys = sorted([k for k in row.keys() if k not in base])
                    fieldnames = base + stats_keys

                    writer = csv.DictWriter(f, fieldnames=fieldnames)

                    # Only write header if we are starting fresh
                    if not file_exists:
                        writer.writeheader()
                        file_exists = True  # Header written

                writer.writerow(row)
                f.flush()  # Save to disk immediately

            except Exception as e:
                logging.error(f"[{name}] Failed {mpio_id}: {e}")

    logging.info(f"[{name}] Run Complete.")


if __name__ == "__main__":
    lib.init_ee()

    # Run Modules sequentially
    run_module_full(whole, "Whole")
    # run_module_full(urban, "Urban")
    # run_module_full(rural, "Rural")
