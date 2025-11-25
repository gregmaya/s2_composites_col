import json
import logging
import os

from google.cloud import storage

# --------------------------------------------------
# --- UNIFIED POST-PROCESSING MANAGER (v2 - With Histogram) ---
# --------------------------------------------------

# --- CONFIGURATION ---
GCS_BUCKET_NAME = "drycap-tiles-colombia"
VERSION_SUFFIX = "producer_q40_nocap_v2"
INITIAL_FAILED_LIST = "failed_q65_audit_dynamic.txt"
OUTPUT_FAILED_LIST = "failed_for_next_stage_q30.txt"
MINIMUM_COVERAGE_THRESHOLD = 0.95
PERFORM_MOVE = False

# --------------------------------------------------
# --- Initialization ---
# --------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(message)s")


def extract_mpio_id_from_filename(filename):
    try:
        parts = filename.split("_m")
        if len(parts) < 2:
            return None
        candidate = parts[-1]
        mpio_id = ""
        for char in candidate:
            if char.isdigit():
                mpio_id += char
            else:
                break
        return mpio_id if len(mpio_id) > 0 else None
    except:
        return None


def is_orphan(filename, failed_ids_set):
    parts = filename.split("_m")
    if len(parts) < 2:
        return False

    candidate = parts[-1]
    digit_string = ""
    for char in candidate:
        if char.isdigit():
            digit_string += char
        else:
            break

    if not digit_string:
        return False

    if digit_string in failed_ids_set:
        return True
    if len(digit_string) > 5:
        if digit_string[:5] in failed_ids_set:
            return True
        if digit_string[:4] in failed_ids_set:
            return True
    return False


def move_gcs_blob(bucket, source_blob_name, destination_blob_name):
    try:
        source_blob = bucket.blob(source_blob_name)
        if not source_blob.exists():
            return False
        bucket.copy_blob(source_blob, bucket, destination_blob_name)
        source_blob.delete()
        return True
    except Exception as e:
        logging.error(f"  -> FAILED to move {source_blob_name}: {e}")
        return False


def generate_text_histogram(data, title, bins=10, min_val=0.0, max_val=1.0):
    """Generates a simple ASCII histogram for a list of float values."""
    if not data:  # FIXED: Added 'data'
        return f"{title}: No data available."

    counts = [0] * bins
    bin_width = (max_val - min_val) / bins

    for val in data:  # FIXED: Added 'data'
        # Clamp value to range
        val = max(min_val, min(max_val, val))
        if val == max_val:
            idx = bins - 1
        else:
            idx = int((val - min_val) / bin_width)
        counts[idx] += 1

    max_count = max(counts) if counts else 0
    scale = 20 / max_count if max_count > 0 else 1

    output = [f"\n--- {title} ---"]
    for i in range(bins):
        low = min_val + (i * bin_width)
        high = min_val + ((i + 1) * bin_width)
        bar = "#" * int(counts[i] * scale)
        # Format: "0.90-1.00 |##### (5)"
        label = f"{low:.2f}-{high:.2f}"
        output.append(f"{label:>11} | {bar:<20} ({counts[i]})")

    return "\n".join(output)


# --------------------------------------------------
# --- Main Post-Processing Logic ---
# --------------------------------------------------
def process_round():
    logging.info("=" * 60)
    logging.info("UNIFIED POST-PROCESSING MANAGER")
    logging.info("=" * 60)
    logging.info(f"Processing: {VERSION_SUFFIX}")
    logging.info(f"Coverage Threshold: {MINIMUM_COVERAGE_THRESHOLD:.0%}")
    logging.info(f"Mode: {'MOVING FILES' if PERFORM_MOVE else 'AUDIT ONLY (DRY RUN)'}")
    logging.info("=" * 60)

    initial_failures = set()
    if os.path.exists(INITIAL_FAILED_LIST):
        with open(INITIAL_FAILED_LIST, "r") as f:
            initial_failures = {line.strip() for line in f if line.strip()}

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)

    logging.info("\n[2/5] Auditing metadata files...")
    metadata_prefix = f"metadata_v9/{VERSION_SUFFIX}/"
    all_meta_blobs = list(bucket.list_blobs(prefix=metadata_prefix))
    metadata_blobs = [
        b
        for b in all_meta_blobs
        if b.name.endswith(".geojson") and "low_coverage" not in b.name
    ]

    logging.info(f"      Found {len(metadata_blobs)} metadata files to audit")

    low_coverage_ids = set()
    successful_count = 0
    metadata_to_move = []

    # --- Data Collection for Histogram ---
    cov19_values = []
    cov23_values = []
    # -------------------------------------

    for i, meta_blob in enumerate(metadata_blobs, 1):
        if i % 50 == 0:
            logging.info(f"      Processing {i}/{len(metadata_blobs)}...")
        try:
            metadata = json.loads(meta_blob.download_as_string())
            props = metadata["features"][0]["properties"]
            mpio_id = str(props["municipality_id"])
            cov19 = props.get("final_coverage_2019", 0)
            cov23 = props.get("final_coverage_2023", 0)

            # Store for histogram
            cov19_values.append(cov19)
            cov23_values.append(cov23)

            if (
                cov19 >= MINIMUM_COVERAGE_THRESHOLD
                and cov23 >= MINIMUM_COVERAGE_THRESHOLD
            ):
                successful_count += 1
            else:
                low_coverage_ids.add(mpio_id)
                metadata_to_move.append((meta_blob, mpio_id, cov19, cov23))
        except Exception as e:
            logging.error(f"      Error processing {meta_blob.name}: {e}")

    logging.info(f"      ✓ {successful_count} passed coverage threshold")
    logging.info(f"      ✗ {len(low_coverage_ids)} failed coverage threshold")

    # --- Scan for Orphan Images ---
    logging.info("\n[3/5] Scanning for orphan image files...")
    images_prefix = f"composites_v9/{VERSION_SUFFIX}/"
    all_image_blobs = list(bucket.list_blobs(prefix=images_prefix))
    orphan_images = []
    for blob in all_image_blobs:
        if "low_coverage" in blob.name or blob.name.endswith("/"):
            continue
        filename = blob.name.split("/")[-1]
        if is_orphan(filename, low_coverage_ids):
            orphan_images.append(blob)
    logging.info(f"      Found {len(orphan_images)} orphan image files")

    # --- Generate Final Report ---
    logging.info("\n[4/5] Generating Final Report...")
    total_failed_ids = initial_failures.union(low_coverage_ids)

    logging.info("\n" + "=" * 60)
    logging.info("FINAL REPORT")
    logging.info("=" * 60)
    logging.info(f"Municipalities processed this round: {len(metadata_blobs)}")
    logging.info(f"  - Passed (≥95% coverage):          {successful_count}")
    logging.info(f"  - Failed (<95% coverage):          {len(low_coverage_ids)}")

    # --- INSERT HISTOGRAMS HERE ---
    # Zoom in on the critical 80% - 100% range
    print(
        generate_text_histogram(
            cov19_values,
            "2019 Coverage Distribution (80-100%)",
            bins=12,
            min_val=0.70,
            max_val=1.0,
        )
    )
    print(
        generate_text_histogram(
            cov23_values,
            "2023 Coverage Distribution (80-100%)",
            bins=12,
            min_val=0.70,
            max_val=1.0,
        )
    )
    # ------------------------------

    # ------------------------------

    logging.info("\nNext Round Planning:")
    logging.info(f"  - Total for next round:            {len(total_failed_ids)}")
    logging.info("=" * 60)

    if PERFORM_MOVE:
        logging.info("\n[5/5] MOVING FILES...")
        moved_meta = 0
        for meta_blob, _, _, _ in metadata_to_move:
            dest_name = meta_blob.name.replace(
                metadata_prefix, f"{metadata_prefix}low_coverage/"
            )
            if move_gcs_blob(bucket, meta_blob.name, dest_name):
                moved_meta += 1
        moved_images = 0
        for img_blob in orphan_images:
            dest_name = img_blob.name.replace(
                images_prefix, f"{images_prefix}low_coverage/"
            )
            if move_gcs_blob(bucket, img_blob.name, dest_name):
                moved_images += 1

        with open(OUTPUT_FAILED_LIST, "w") as f:
            for mpio_id in sorted(list(total_failed_ids)):
                f.write(f"{mpio_id}\n")

        logging.info(f"\n✓ Wrote {len(total_failed_ids)} IDs to {OUTPUT_FAILED_LIST}")
        logging.info("POST-PROCESSING COMPLETE")
    else:
        logging.info("\n[5/5] AUDIT COMPLETE (No files moved)")
        logging.info("To execute the moves, set PERFORM_MOVE = True and run again.")


if __name__ == "__main__":
    process_round()
