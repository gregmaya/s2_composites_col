import json
import logging
import os

from google.cloud import storage

# --- CONFIGURATION ---
GCS_BUCKET_NAME = "drycap-tiles-colombia"
FAILED_LIST_FILE = "failed_for_next_stage_q30.txt"
OUTPUT_JSON_FILE = "master_composites_index_v3.json"

SOURCES = {
    "q65": {
        "images": "composites_v9/producer_q65_nocap_v2/",
        "meta": "metadata_v9/producer_q65_nocap_v2/",
    },
    "q50": {
        "images": "composites_v9/producer_q50_nocap_v2/",
        "meta": "metadata_v9/producer_q50_nocap_v2/",
    },
    "q40": {
        "images": "composites_v9/producer_q40_nocap_v2/",
        "meta": "metadata_v9/producer_q40_nocap_v2/",
    },
}

ACCEPTABLE_COV = 0.90

logging.basicConfig(level=logging.INFO, format="%(message)s")


def extract_id(filename):
    try:
        part = filename.split("_m")[-1]
        mid = ""
        for c in part:
            if c.isdigit():
                mid += c
            else:
                break
        return mid
    except:
        return None


def get_metadata_content(bucket, prefix, mpio_id):
    # Try main folder
    blobs = list(bucket.list_blobs(prefix=prefix))
    target = None
    for b in blobs:
        if (
            f"_m{mpio_id}." in b.name
            and b.name.endswith(".geojson")
            and "low_coverage" not in b.name
        ):
            target = b
            break
    # Try low_coverage folder
    if not target:
        blobs = list(bucket.list_blobs(prefix=prefix + "low_coverage/"))
        for b in blobs:
            if f"_m{mpio_id}." in b.name and b.name.endswith(".geojson"):
                target = b
                break

    if target:
        try:
            return json.loads(target.download_as_string())
        except:
            return None
    return None


def find_images_for_id(bucket, prefix, mpio_id):
    """Finds all tile images for a given ID by partial string matching."""
    images = []

    # Define the search pattern.
    # We look for '_m' + ID. This avoids matching '13458' inside '513458'.
    target_pattern = f"_m{mpio_id}"

    # 1. Search Main Folder
    blobs = list(bucket.list_blobs(prefix=prefix))
    for b in blobs:
        # strict check: must be a TIF and contain the ID pattern
        if b.name.endswith(".tif") and target_pattern in b.name:
            images.append(f"gs://{GCS_BUCKET_NAME}/{b.name}")

    # 2. Search Low Coverage Folder (if applicable)
    # (Only needed if we are in a fallback scenario, but safe to check)
    blobs_low = list(bucket.list_blobs(prefix=prefix + "low_coverage/"))
    for b in blobs_low:
        if b.name.endswith(".tif") and target_pattern in b.name:
            images.append(f"gs://{GCS_BUCKET_NAME}/{b.name}")

    return sorted(images)


def generate_master_index():
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # 1. Load the 70 Low Coverage IDs
    low_cov_ids = set()
    if os.path.exists(os.path.join(script_dir, FAILED_LIST_FILE)):
        with open(os.path.join(script_dir, FAILED_LIST_FILE), "r") as f:
            low_cov_ids = {line.strip() for line in f if line.strip()}
    logging.info(f"Loaded {len(low_cov_ids)} Low Coverage IDs.")

    # 2. Identify "Success" IDs (High Coverage)
    # We scan the metadata folders of q65, q50, q40 (Main only) to find passed audits.
    success_map = {}  # ID -> Quality (e.g. "12345": "q65")

    # Priority scan: q65 -> q50 -> q40
    for q in ["q65", "q50", "q40"]:
        prefix = SOURCES[q]["meta"]
        blobs = list(bucket.list_blobs(prefix=prefix))
        for b in blobs:
            if b.name.endswith(".geojson") and "low_coverage" not in b.name:
                mid = extract_id(b.name)
                if mid and mid not in low_cov_ids and mid not in success_map:
                    success_map[mid] = q

    logging.info(f"Identified {len(success_map)} Success IDs from metadata.")

    # 3. Combine into Master List
    all_target_ids = sorted(list(low_cov_ids.union(set(success_map.keys()))))
    logging.info(f"Total Unique Municipalities to Process: {len(all_target_ids)}")

    master_list = []

    for i, mpio_id in enumerate(all_target_ids, 1):
        if i % 50 == 0:
            logging.info(f"Processing {i}/{len(all_target_ids)}...")

        winner = None
        reason = ""

        # CASE A: Low Coverage List (Use Waterfall)
        if mpio_id in low_cov_ids:
            meta65 = get_metadata_content(bucket, SOURCES["q65"]["meta"], mpio_id)
            meta50 = get_metadata_content(bucket, SOURCES["q50"]["meta"], mpio_id)
            meta40 = get_metadata_content(bucket, SOURCES["q40"]["meta"], mpio_id)

            def check(m):
                if not m:
                    return False
                p = m["features"][0]["properties"]
                return (
                    p.get("final_coverage_2019", 0) >= ACCEPTABLE_COV
                    and p.get("final_coverage_2023", 0) >= ACCEPTABLE_COV
                )

            if check(meta65):
                winner = "q65"
                reason = "Strict Waterfall Pass"
            elif check(meta50):
                winner = "q50"
                reason = "Strict Waterfall Pass"
            elif check(meta40):
                winner = "q40"
                reason = "Strict Waterfall Pass"
            else:
                winner = "q40"
                reason = "Best Available Fallback"

        # CASE B: Success List (Use Pre-identified Quality)
        else:
            winner = success_map[mpio_id]
            reason = "Standard Audit Pass"

        # --- BUILD ENTRY ---
        image_files = find_images_for_id(bucket, SOURCES[winner]["images"], mpio_id)
        meta_content = get_metadata_content(bucket, SOURCES[winner]["meta"], mpio_id)
        properties = meta_content["features"][0]["properties"] if meta_content else {}

        entry = {
            "id": mpio_id,
            "selected_quality": winner,
            "selection_reason": reason,
            "image_files": image_files,
            "properties": properties,
        }
        master_list.append(entry)

    # 4. Output
    out_path = os.path.join(script_dir, OUTPUT_JSON_FILE)
    with open(out_path, "w") as f:
        json.dump(master_list, f, indent=2)

    logging.info(f"Master Index saved to: {out_path}")
    logging.info(f"Total Entries: {len(master_list)}")


if __name__ == "__main__":
    generate_master_index()
