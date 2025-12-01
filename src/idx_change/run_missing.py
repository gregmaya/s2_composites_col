import csv
import json
import logging

import index_lib as lib
from runners import roads_frontier

# CONFIG
MASTER_JSON = "data/master_composites_index_v3.json"
OUTPUT_FILE = "data/results/roads_results.csv"  # We will APPEND to this
MISSING_IDS = ["99773", "50568", "18753"]

logging.basicConfig(level=logging.INFO)


def run():
    lib.init_ee()

    # Load Master
    with open(MASTER_JSON, "r") as f:
        all_data = json.load(f)

    # Filter for missing
    targets = [d for d in all_data if d["id"] in MISSING_IDS]

    logging.info(f"Found {len(targets)} entries to process.")

    # Process
    new_rows = []
    for entry in targets:
        res = roads_frontier.process(entry)
        if res:
            new_rows.extend(res)

    # Append to CSV
    if new_rows:
        # Get headers from existing file to ensure match
        with open(OUTPUT_FILE, "r") as f:
            reader = csv.reader(f)
            headers = next(reader)

        with open(OUTPUT_FILE, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            for row in new_rows:
                # Ensure row only has keys that exist in headers
                # (In case frontier produces extra/fewer cols, unlikely)
                clean_row = {k: row.get(k, "") for k in headers}
                writer.writerow(clean_row)

        logging.info(f"Successfully appended {len(new_rows)} rows to {OUTPUT_FILE}")
    else:
        logging.warning("No new rows generated.")


if __name__ == "__main__":
    run()
