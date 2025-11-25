import csv
import json
import logging
import os

from google.cloud import storage

# --- CONFIGURATION ---
GCS_BUCKET_NAME = "drycap-tiles-colombia"
FAILED_LIST_FILE = "failed_for_next_stage_q30.txt"
OUTPUT_CSV_FILE = "candidate_selection_results.csv"

SOURCES = {
    "q65": "metadata_v9/producer_q65_nocap_v2/low_coverage/",
    "q50": "metadata_v9/producer_q50_nocap_v2/low_coverage/",
    "q40": "metadata_v9/producer_q40_nocap_v2/low_coverage/",
}

# --- STRICT THRESHOLD ---
ACCEPTABLE_COV = 0.90  # Both years must be >= 90%

logging.basicConfig(level=logging.INFO, format="%(message)s")


def get_coverage_stats(bucket, prefix, mpio_id):
    blobs = list(bucket.list_blobs(prefix=prefix))
    target_blob = None
    for b in blobs:
        if f"_m{mpio_id}." in b.name:
            target_blob = b
            break
    if not target_blob:
        return None

    try:
        data = json.loads(target_blob.download_as_string())
        props = data["features"][0]["properties"]
        c19 = props.get("final_coverage_2019", 0)
        c23 = props.get("final_coverage_2023", 0)
        return {"c19": c19, "c23": c23}
    except:
        return None


def select_best_candidate():
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    list_path = os.path.join(script_dir, FAILED_LIST_FILE)
    csv_path = os.path.join(script_dir, OUTPUT_CSV_FILE)

    with open(list_path, "r") as f:
        target_ids = [line.strip() for line in f if line.strip()]

    logging.info(f"Analyzing {len(target_ids)} municipalities...")
    logging.info(
        f"Strategy: Strict Waterfall. BOTH years must be >= {ACCEPTABLE_COV:.0%}."
    )
    logging.info("-" * 85)

    counts = {"q65": 0, "q50": 0, "q40": 0}
    results = []  # List to store data for CSV

    for mpio_id in target_ids:
        s65 = get_coverage_stats(bucket, SOURCES["q65"], mpio_id)
        s50 = get_coverage_stats(bucket, SOURCES["q50"], mpio_id)
        s40 = get_coverage_stats(bucket, SOURCES["q40"], mpio_id)

        # --- STRICT LOGIC ---
        winner = "q40"
        reason = "Best Available (Failed Strict)"  # Default

        # 1. Check q65
        if s65 and s65["c19"] >= ACCEPTABLE_COV and s65["c23"] >= ACCEPTABLE_COV:
            winner = "q65"
            reason = "High Quality Pass"

        # 2. Check q50 (if q65 failed)
        elif s50 and s50["c19"] >= ACCEPTABLE_COV and s50["c23"] >= ACCEPTABLE_COV:
            winner = "q50"
            reason = "Medium Quality Pass"

        # 3. Check q40 (if q50 failed)
        elif s40 and s40["c19"] >= ACCEPTABLE_COV and s40["c23"] >= ACCEPTABLE_COV:
            winner = "q40"
            reason = "Low Quality Pass"

        # 4. Desperation (None passed strict check)
        else:

            def min_cov(s):
                return min(s["c19"], s["c23"]) if s else 0

            best_val = max(min_cov(s65), min_cov(s50), min_cov(s40))
            if best_val == min_cov(s65):
                winner = "q65"
            elif best_val == min_cov(s50):
                winner = "q50"
            else:
                winner = "q40"

        counts[winner] += 1

        # Helper for readable values
        def val(s, year):
            return s[year] if s else 0.0

        # Store result row
        results.append(
            {
                "id": mpio_id,
                "winner": winner,
                "reason": reason,
                "q65_19": val(s65, "c19"),
                "q65_23": val(s65, "c23"),
                "q50_19": val(s50, "c19"),
                "q50_23": val(s50, "c23"),
                "q40_19": val(s40, "c19"),
                "q40_23": val(s40, "c23"),
            }
        )

        # Print to log
        def fmt(s):
            return f"{s['c19']:.0%}/{s['c23']:.0%}" if s else "N/A"

        logging.info(
            f"{mpio_id:<6} | {fmt(s65):<13} | {fmt(s50):<13} | {fmt(s40):<13} | {winner:<6} | {reason}"
        )

    # --- WRITE CSV ---
    with open(csv_path, "w", newline="") as csvfile:
        fieldnames = [
            "id",
            "winner",
            "reason",
            "q65_19",
            "q65_23",
            "q50_19",
            "q50_23",
            "q40_19",
            "q40_23",
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    logging.info("-" * 85)
    logging.info(f"CSV Exported to: {csv_path}")
    logging.info("FINAL DECISION SUMMARY")
    logging.info(f"q65: {counts['q65']}")
    logging.info(f"q50: {counts['q50']}")
    logging.info(f"q40: {counts['q40']}")
    logging.info("-" * 85)


if __name__ == "__main__":
    select_best_candidate()
