import logging
import os

# --------------------------------------------------
# --- LIST MERGER SCRIPT ---
# --------------------------------------------------

# Input 1: The initial audit failures (The "Impossible 13")
INITIAL_FAILURES_FILE = "failed_q65_audit_dynamic.txt"

# Input 2 (and Output): The failures from the Producer run (The "Low Coverage 143")
TARGET_LIST_FILE = "failed_for_next_stage_q50.txt"

logging.basicConfig(level=logging.INFO, format="%(message)s")


def merge_lists():
    # 1. Read Initial Failures
    initial_set = set()
    if os.path.exists(INITIAL_FAILURES_FILE):
        with open(INITIAL_FAILURES_FILE, "r") as f:
            initial_set = {line.strip() for line in f if line.strip()}
        logging.info(
            f"Loaded {len(initial_set)} IDs from initial failures ({INITIAL_FAILURES_FILE})"
        )
    else:
        logging.warning(f"Initial failures file not found: {INITIAL_FAILURES_FILE}")

    # 2. Read Producer Failures
    producer_set = set()
    if os.path.exists(TARGET_LIST_FILE):
        with open(TARGET_LIST_FILE, "r") as f:
            producer_set = {line.strip() for line in f if line.strip()}
        logging.info(
            f"Loaded {len(producer_set)} IDs from producer failures ({TARGET_LIST_FILE})"
        )
    else:
        logging.warning(f"Target list file not found: {TARGET_LIST_FILE}")

    # 3. Merge
    combined_set = initial_set.union(producer_set)

    logging.info("-" * 30)
    logging.info(f"Total Unique IDs Combined: {len(combined_set)}")
    logging.info("-" * 30)

    # 4. Write Back
    with open(TARGET_LIST_FILE, "w") as f:
        for mpio_id in sorted(list(combined_set)):
            f.write(f"{mpio_id}\n")

    logging.info(f"Successfully updated {TARGET_LIST_FILE} with the complete list.")


if __name__ == "__main__":
    merge_lists()
