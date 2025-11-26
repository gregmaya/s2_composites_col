import csv
import json
import logging
import os

import index_lib as lib

# Import your new runner
from runners import whole

# --- CONFIG ---
PILOT_SIZE = 10
MASTER_JSON = "data/master_composites_index_v3.json"
OUTPUT_DIR = "data/pilot_results"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")


def save_csv(results, filename):
    if not results:
        logging.warning(f"No results to save for {filename}")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filepath = os.path.join(OUTPUT_DIR, filename)

    # Sort keys: id, type, quality, then alphabetical stats
    base_keys = ["id", "type", "quality"]
    other_keys = sorted([k for k in results[0].keys() if k not in base_keys])
    keys = base_keys + other_keys

    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(results)
    logging.info(f"Saved {len(results)} rows to {filepath}")


def run_pilot_module(module, name):
    logging.info(f"--- Running Pilot: {name} ---")

    # Load Master JSON
    try:
        with open(MASTER_JSON, "r") as f:
            data = json.load(f)
    except FileNotFoundError:
        logging.error(f"Could not find {MASTER_JSON}. Check path.")
        return

    # Slice for Pilot
    pilot_data = data[:PILOT_SIZE]
    results = []

    for i, entry in enumerate(pilot_data):
        try:
            logging.info(f"[{name}] Processing {i + 1}/{PILOT_SIZE}: {entry['id']}...")
            row = module.process(entry)
            if row:
                results.append(row)
            else:
                logging.warning(f"[{name}] Empty result for {entry['id']}")
        except Exception as e:
            logging.error(f"[{name}] Failed {entry['id']}: {e}")

    # Save
    save_csv(results, f"{name.lower()}_pilot.csv")


if __name__ == "__main__":
    lib.init_ee()

    # Run 'Whole' module
    run_pilot_module(whole, "Whole")
