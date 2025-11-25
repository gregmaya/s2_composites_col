import logging
import os
import re
from collections import Counter

import ee

# --------------------------------------------------
# --- ON-DEMAND STATUS CHECKER ---
# --------------------------------------------------

# This script will read the specified log file to find all task IDs.
# Ensure this path is correct.
LOG_FILE_TO_PARSE = "producer_q40_nocap.log"

# --- Leave the rest of the script as is ---

# --------------------------------------------------
# --- Initialization & Main Logic
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def parse_task_ids_from_log(log_file_path):
    """Reads a log file and extracts all GEE task IDs."""
    if not os.path.exists(log_file_path):
        logging.error(f"Log file not found: {log_file_path}")
        return []

    # This regex is designed to find all 24-character GEE task IDs in the log.
    task_id_pattern = re.compile(r"\b([A-Z0-9]{24})\b")
    task_ids = set()

    with open(log_file_path, "r") as f:
        for line in f:
            matches = task_id_pattern.findall(line)
            for task_id in matches:
                task_ids.add(task_id)

    logging.info(f"Found {len(task_ids)} unique task IDs in {log_file_path}")
    return list(task_ids)


def check_tasks_on_demand(task_ids_to_check):
    """
    Connects to GEE, gets the status of all tasks, prints a summary,
    and reports details on any failures.
    """
    if not task_ids_to_check:
        logging.warning("No task IDs found in the log file. Exiting.")
        return

    try:
        logging.info("Connecting to Google Earth Engine...")
        ee.Initialize(opt_url="https://earthengine-highvolume.googleapis.com")
        logging.info("GEE initialized successfully.")

        logging.info("Fetching status for all recent tasks... (This may take a moment)")
        all_tasks = ee.batch.Task.list()
        task_map = {task.id: task for task in all_tasks}

        # --- Data Collection ---
        statuses = []
        failed_tasks_details = []

        for task_id in task_ids_to_check:
            task = task_map.get(task_id)
            if not task:
                # If a task isn't in the list, it's likely very old and completed successfully.
                # We'll mark it as 'UNKNOWN' for now to be safe.
                statuses.append("UNKNOWN (Not in recent list)")
                continue

            status_dict = task.status()
            state = status_dict.get("state")
            statuses.append(state)

            if state == "FAILED":
                failed_tasks_details.append(status_dict)

        # --- Reporting ---
        logging.info("--- On-Demand Task Status Report ---")

        # 1. High-Level Summary
        summary = Counter(statuses)
        summary_str = ", ".join([f"{k}: {v}" for k, v in summary.items()])
        print(f"\nOverall Summary: {summary_str}\n")

        # 2. Detailed Failure Report
        if failed_tasks_details:
            logging.warning(f"--- Found {len(failed_tasks_details)} FAILED Tasks ---")
            for status in failed_tasks_details:
                description = status.get("description", "N/A")
                task_id = status.get("id", "N/A")
                error_message = status.get(
                    "error_message", "No error message provided."
                )
                print(f"  - Task: {description} (ID: {task_id})")
                print(f"    Reason: {error_message}\n")
        else:
            logging.info("--- No FAILED tasks found among the monitored set. ---")

    except Exception as e:
        logging.error(f"An error occurred while checking tasks: {e}", exc_info=True)


if __name__ == "__main__":
    task_ids = parse_task_ids_from_log(LOG_FILE_TO_PARSE)
    check_tasks_on_demand(task_ids)
    logging.info("Status check complete.")
