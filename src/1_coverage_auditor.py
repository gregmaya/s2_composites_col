import logging

import ee

# --------------------------------------------------
# --- "THE AUDITOR" SCRIPT CONFIGURATION ---
# --------------------------------------------------
# This script can get its list of municipalities in two ways:
# 1. DYNAMICALLY (for Stage 1): It will use the filter below.
# 2. MANUALLY (for Stage 2+): Comment out the dynamic section and
#    un-comment the MPIO_TO_PROCESS list, pasting in the failed IDs.

# --- DYNAMIC INPUT (for Stage 1) ---
USE_DYNAMIC_LIST = True
MPIO_PROPERTY_FILTER = "selected_m"
MPIO_PROPERTY_VALUE = True

# --- MANUAL INPUT (for Stage 2 and beyond) ---
# MPIO_TO_PROCESS = [
#     # Paste the list of failed IDs from the previous audit CSV here
# ]

# The quality threshold to check for this audit.
# STAGE 1 -> 0.65
# STAGE 2 -> 0.50
CLEAR_THRESHOLD = 0.65

# A suffix for the output CSV to identify this stage.
VERSION_SUFFIX = "q65_audit_dynamic"

# --- Asset and Bucket Configuration ---
MUN_BOUNDARY_ASSET = "projects/small-towns-col/assets/mun2018_simpl10"
GCS_BUCKET = "drycap-tiles-colombia"  # IMPORTANT: Your bucket name

# --- GEE & Pipeline Parameters ---
S2_COLLECTION = "COPERNICUS/S2_SR_HARMONIZED"
CS_PLUS_COLLECTION = "GOOGLE/CLOUD_SCORE_PLUS/V1/S2_HARMONIZED"
CS_PLUS_QA_BAND = "cs_cdf"
S2_MAX_CLOUD_PCT = 80
MIN_COVERAGE_FRACTION = 0.90
PIX_SIZE = 10
CRS = "EPSG:3116"

# --------------------------------------------------
# --- Initialization & Main Logic
# --------------------------------------------------
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
try:
    ee.Initialize(opt_url="https://earthengine-highvolume.googleapis.com")
    logging.info("GEE initialized.")
except Exception:
    ee.Authenticate()
    ee.Initialize(opt_url="https://earthengine-highvolume.googleapis.com")


def check_coverage(mpio_id):
    """
    Constructs a server-side feature that contains the municipality ID and its
    potential coverage for 2019 and 2023 at the given threshold.
    """
    try:
        feat = ee.Feature(
            ee.FeatureCollection(MUN_BOUNDARY_ASSET)
            .filter(ee.Filter.eq("mpio_ccnct", mpio_id))
            .first()
        )
        geom = feat.geometry()

        s2_with_cs = ee.ImageCollection(S2_COLLECTION).linkCollection(
            ee.ImageCollection(CS_PLUS_COLLECTION), [CS_PLUS_QA_BAND]
        )
        base19 = (
            s2_with_cs.filterBounds(geom)
            .filterDate("2019-01-01", "2019-12-31")
            .filter(ee.Filter.lte("CLOUDY_PIXEL_PERCENTAGE", S2_MAX_CLOUD_PCT))
        )
        base23 = (
            s2_with_cs.filterBounds(geom)
            .filterDate("2023-01-01", "2023-12-31")
            .filter(ee.Filter.lte("CLOUDY_PIXEL_PERCENTAGE", S2_MAX_CLOUD_PCT))
        )

        total_area = geom.area(1)

        def get_coverage_fraction(collection):
            """Performs the lightweight composite and area calculation."""
            lightweight_comp = (
                collection.map(
                    lambda img: img.updateMask(
                        img.select(CS_PLUS_QA_BAND).gte(CLEAR_THRESHOLD)
                    )
                )
                .select("B4")
                .median()
            )
            pixel_area = ee.Image.pixelArea()
            covered_area = (
                pixel_area.updateMask(lightweight_comp.mask())
                .reduceRegion(
                    reducer=ee.Reducer.sum(),
                    geometry=geom,
                    scale=PIX_SIZE,
                    maxPixels=1e13,
                )
                .get("area")
            )
            return ee.Number(covered_area).divide(total_area)

        cov19 = get_coverage_fraction(base19)
        cov23 = get_coverage_fraction(base23)

        passes_check = cov19.gte(MIN_COVERAGE_FRACTION).And(
            cov23.gte(MIN_COVERAGE_FRACTION)
        )

        return ee.Feature(
            None,
            {
                "municipality_id": mpio_id,
                "coverage_2019": cov19,
                "coverage_2023": cov23,
                "passes_check": passes_check,
            },
        )
    except Exception as e:
        logging.error(f"Failed to construct feature for {mpio_id}: {e}")
        return None


# --------------------------------------------------
# --- Main Execution Loop
# --------------------------------------------------
if __name__ == "__main__":
    if "USE_DYNAMIC_LIST" in locals() and USE_DYNAMIC_LIST:
        logging.info(
            f"Fetching municipality list dynamically from asset where '{MPIO_PROPERTY_FILTER}' is {MPIO_PROPERTY_VALUE}..."
        )
        # This is a SAFE use of getInfo() as it only fetches a list of IDs (strings/numbers), not geometry.
        municipality_collection = ee.FeatureCollection(MUN_BOUNDARY_ASSET).filter(
            ee.Filter.eq(MPIO_PROPERTY_FILTER, MPIO_PROPERTY_VALUE)
        )
        mpio_to_process = municipality_collection.aggregate_array(
            "mpio_ccnct"
        ).getInfo()
        logging.info(f"Found {len(mpio_to_process)} municipalities to process.")
    else:
        mpio_to_process = MPIO_TO_PROCESS
        logging.info(
            f"Using manually defined list of {len(mpio_to_process)} municipalities."
        )

    logging.info("Starting Asynchronous Auditor.")
    logging.info(f"Using CLEAR_THRESHOLD: {CLEAR_THRESHOLD}")
    logging.info(f"Output suffix: {VERSION_SUFFIX}")

    features_to_export = []
    for mpio_id in mpio_to_process:
        feature = check_coverage(mpio_id)
        if feature:
            features_to_export.append(feature)

    if not features_to_export:
        logging.error("No valid features could be constructed. Aborting.")
    else:
        feature_collection = ee.FeatureCollection(features_to_export)

        output_filename = f"coverage_audit_{VERSION_SUFFIX}"
        task = ee.batch.Export.table.toCloudStorage(
            collection=feature_collection,
            description=output_filename,
            bucket=GCS_BUCKET,
            fileNamePrefix=f"auditor_results/{VERSION_SUFFIX}/{output_filename}",
            fileFormat="CSV",
        )
        task.start()

        logging.info(f"SUBMITTED Auditor Task: {task.id}")
        logging.info(
            f"Results for all {len(mpio_to_process)} municipalities will be in a single CSV file in your GCS bucket when complete."
        )

    logging.info("Auditor script finished submitting tasks.")
