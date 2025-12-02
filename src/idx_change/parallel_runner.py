import csv
import json
import logging
import os
from multiprocessing import Pool

import ee

# Import modules

# --- CONFIG ---
MASTER_JSON = "data/master_composites_index_v3.json"
OUTPUT_DIR = "data/results"
NUM_WORKERS = 8  # Adjust based on your CPU cores (usually 4-8 is safe)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")


def init_worker():
    """Initialize Earth Engine in each worker process."""
    try:
        ee.Initialize()
    except:
        ee.Authenticate()
        ee.Initialize()


def process_wrapper(args):
    entry, module_name = args

    if module_name == "urban":
        from runners import urban as mod
    elif module_name == "whole":
        from runners import whole as mod
    elif module_name == "rural":
        from runners import rural as mod
    elif module_name == "roads":
        from runners import roads as mod
    else:
        raise ValueError(f"Unknown module: {module_name}")

    try:
        return mod.process(entry)
    except Exception as e:
        return {"error": str(e), "id": entry["id"]}


def run_parallel(module_name, workers=NUM_WORKERS):
    logging.info(f"--- Starting Parallel Run: {module_name} with {workers} workers ---")

    with open(MASTER_JSON, "r") as f:
        data = json.load(f)  # [:10]  # For testing, limit to 10 entries

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filepath = os.path.join(OUTPUT_DIR, f"{module_name.lower()}_results.csv")

    # Prepare args
    tasks = [(entry, module_name) for entry in data]

    results = []
    processed_count = 0
    errors = 0

    # Start Pool
    with Pool(processes=workers, initializer=init_worker) as pool:
        # Use imap_unordered for streaming results as they finish
        for result in pool.imap_unordered(process_wrapper, tasks):
            processed_count += 1
            if processed_count % 10 == 0:
                logging.info(f"[{module_name}] Progress: {processed_count}/{len(data)}")

            if result and "error" not in result:
                if isinstance(result, list):
                    results.extend(result)  # Flatten: [a, b] + [c, d] -> [a, b, c, d]
                else:
                    results.append(result)  # Standard: [a, b] + c -> [a, b, c]

    # Save Results
    if results:
        # Define priority order for metadata columns
        base_cols = ["id", "type", "subtype", "location", "quality", "selection_reason"]

        # Get all keys present in the first result
        first_keys = results[0].keys()

        # 1. Extract found base columns in order
        header = [c for c in base_cols if c in first_keys]

        # 2. Extract all other stats columns (sorted)
        stats = sorted([k for k in first_keys if k not in base_cols])

        header += stats

        with open(filepath, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()
            writer.writerows(results)

    logging.info(
        f"Run Complete. Saved {len(results)} rows to {filepath}. Errors: {errors}"
    )


if __name__ == "__main__":
    # Run Urban in Parallel
    run_parallel("whole", workers=NUM_WORKERS)
