import logging
import os

import ee

# --------------------------------------------------
# --- "THE PRODUCER" SCRIPT (v3 - PRODUCTION READY) ---
# --------------------------------------------------
EXPORT = True  # Set to True for the production run.

# The input file from the initial Auditor run.
INPUT_LIST_FILE = "passed_q65_audit_dynamic.txt"
# --- CHANGE 1: Point to the new list of 156 failed municipalities ---
INPUT_LIST_FILE = "failed_for_next_stage_q50.txt"
# --- CHANGE 1: Point to the new list of 101 failed municipalities ---
INPUT_LIST_FILE = "failed_for_next_stage_q40.txt"

# A new suffix to clearly identify this improved production run.
VERSION_SUFFIX = "producer_q65_nocap_v2"
# --- CHANGE 2: Update the suffix to identify this as the "q50" run ---
VERSION_SUFFIX = "producer_q50_nocap_v2"
# --- CHANGE 2: Update the suffix to identify this as the "q40" run ---
VERSION_SUFFIX = "producer_q40_nocap_v2"

# ---
CLEAR_THRESHOLD = 0.65
# --- CHANGE 3: Lower the threshold to allow more pixels (0.65 -> 0.50) ---
CLEAR_THRESHOLD = 0.50
# --- CHANGE 3: Lower the threshold to allow more pixels (0.50 -> 0.40) ---
CLEAR_THRESHOLD = 0.40

DRYNESS_LOWER_PERCENTILE = 30
DRYNESS_UPPER_PERCENTILE = 70
MUN_BOUNDARY_ASSET = "projects/small-towns-col/assets/mun2018_simpl10"
GCS_BUCKET = "drycap-tiles-colombia"
S2_COLLECTION = "COPERNICUS/S2_SR_HARMONIZED"
CS_PLUS_COLLECTION = "GOOGLE/CLOUD_SCORE_PLUS/V1/S2_HARMONIZED"
CS_PLUS_QA_BAND = "cs_cdf"
S2_MAX_CLOUD_PCT = 80
CRS = "EPSG:3116"
PIX_SIZE = 10
SENTINEL_NODATA = 9
NDMI_SCORE_SCALE = 300


# --------------------------------------------------
# --- Initialization & Helper Functions ---
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

# --- ROBUST FILE PATH HANDLING ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_LIST_FILE = os.path.join(SCRIPT_DIR, INPUT_LIST_FILE)


def add_all_indices(image):
    return image.addBands(
        image.normalizedDifference(["B8", "B4"]).rename("NDVI")
    ).addBands(image.normalizedDifference(["B8", "B11"]).rename("NDMI"))


def set_ndmi_property(image, geometry):
    ndmi = image.normalizedDifference(["B8", "B11"]).rename("ndmi_med")
    ndmi_dict = ndmi.reduceRegion(
        reducer=ee.Reducer.median(),
        geometry=geometry,
        scale=NDMI_SCORE_SCALE,
        bestEffort=True,
    )
    return image.set(
        "ndmi_med",
        ee.Algorithms.If(
            ndmi_dict.contains("ndmi_med"), ndmi_dict.get("ndmi_med"), SENTINEL_NODATA
        ),
    )


def create_final_composite(collection, geometry):
    # Set a quarter property on each image for metadata logging, even though we don't cap.
    def set_quarter_property(img):
        return img.set(
            "quarter",
            ee.Date(img.get("system:time_start"))
            .get("month")
            .subtract(1)
            .divide(3)
            .floor()
            .add(1),
        )

    collection_with_q = collection.map(set_quarter_property)

    masked = collection_with_q.map(
        lambda img: img.updateMask(img.select(CS_PLUS_QA_BAND).gte(CLEAR_THRESHOLD))
    )
    final_comp = add_all_indices(
        masked.select(["B2", "B3", "B4", "B8", "B11", "B12"]).median().clip(geometry)
    ).toFloat()

    # Final Verified Coverage Calculation
    pixel_area = ee.Image.pixelArea()
    coverage_fraction = ee.Number(
        pixel_area.updateMask(final_comp.select("B4").mask())
        .reduceRegion(
            reducer=ee.Reducer.sum(), geometry=geometry, scale=PIX_SIZE, maxPixels=1e13
        )
        .get("area")
    ).divide(geometry.area(1))

    # Final Per-Quarter Image Count for metadata
    def count_in_quarter(q):
        return collection_with_q.filter(ee.Filter.eq("quarter", q)).size()

    quarter_counts = ee.Dictionary(
        {
            "q1_img_count": count_in_quarter(1),
            "q2_img_count": count_in_quarter(2),
            "q3_img_count": count_in_quarter(3),
            "q4_img_count": count_in_quarter(4),
        }
    )

    return final_comp, coverage_fraction, quarter_counts


# --------------------------------------------------
# --- Main Asynchronous Processing Function ---
# --------------------------------------------------
def process_municipality(mpio_id_str):
    try:
        mpio_id_num = int(mpio_id_str)
        feat = ee.Feature(
            ee.FeatureCollection(MUN_BOUNDARY_ASSET)
            .filter(ee.Filter.eq("mpio_ccnct", mpio_id_num))
            .first()
        )

        # This check should now pass for all IDs in the 'passed' list
        if not feat.getInfo():
            logging.error(
                f"DATA MISMATCH: Municipality ID '{mpio_id_str}' was in the passed list but not found in the asset. Skipping."
            )
            return

        geom = feat.geometry()
        mpio_name = feat.get("mpio_cnmbr")
        mpio_area = geom.area(1).divide(1e6)

        logging.info(f"Constructing graph for municipality ID: {mpio_id_str}")

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

        # NO CAPPING is applied. We proceed directly to adding the NDMI property.
        ndmi_scored19 = base19.map(lambda img: set_ndmi_property(img, geom))
        ndmi_scored23 = base23.map(lambda img: set_ndmi_property(img, geom))

        clean19 = ndmi_scored19.filter(ee.Filter.lt("ndmi_med", SENTINEL_NODATA))
        clean23 = ndmi_scored23.filter(ee.Filter.lt("ndmi_med", SENTINEL_NODATA))

        # Unified Dryness Window Logic
        combined_collection = clean19.merge(clean23)

        # Check for empty collections before trying to calculate percentiles
        if combined_collection.size().getInfo() == 0:
            logging.warning(
                f"SKIPPING ID {mpio_id_str}: No valid images found after initial cloud/NDMI scoring. No output will be generated."
            )
            return

        ndmi_values = combined_collection.aggregate_array("ndmi_med")
        dryness_bounds = ndmi_values.reduce(
            ee.Reducer.percentile([DRYNESS_LOWER_PERCENTILE, DRYNESS_UPPER_PERCENTILE])
        )
        ndmi_lower_bound = ee.Number(
            ee.Dictionary(dryness_bounds).get(f"p{DRYNESS_LOWER_PERCENTILE}")
        )
        ndmi_upper_bound = ee.Number(
            ee.Dictionary(dryness_bounds).get(f"p{DRYNESS_UPPER_PERCENTILE}")
        )

        dryness_filter = ee.Filter.And(
            ee.Filter.gte("ndmi_med", ndmi_lower_bound),
            ee.Filter.lte("ndmi_med", ndmi_upper_bound),
        )
        final_dry19 = clean19.filter(dryness_filter)
        final_dry23 = clean23.filter(dryness_filter)

        # Another check to prevent silent failures if the dryness window is too restrictive
        if final_dry19.size().getInfo() == 0 or final_dry23.size().getInfo() == 0:
            logging.warning(
                f"SKIPPING ID {mpio_id_str}: Final image collection was empty after dryness filter. No output will be generated."
            )
            return

        comp19, cov19, counts19 = create_final_composite(final_dry19, geom)
        comp23, cov23, counts23 = create_final_composite(final_dry23, geom)

        pair_image = comp19.rename(
            comp19.bandNames().map(lambda b: ee.String(b).cat("_2019"))
        ).addBands(
            comp23.rename(comp23.bandNames().map(lambda b: ee.String(b).cat("_2023")))
        )
        logging.info(f"Graph constructed for ID: {mpio_id_str}.")

        if EXPORT:
            base_filename = f"s2_comp_v9_{VERSION_SUFFIX}_m{mpio_id_str}"

            metadata = (
                ee.Dictionary(
                    {
                        "municipality_id": mpio_id_num,
                        "municipality_name": mpio_name,
                        "area_km2": mpio_area,
                        "clear_threshold_used": CLEAR_THRESHOLD,
                        "dryness_lower_percentile": DRYNESS_LOWER_PERCENTILE,
                        "dryness_upper_percentile": DRYNESS_UPPER_PERCENTILE,
                        "ndmi_lower_bound": ndmi_lower_bound,
                        "ndmi_upper_bound": ndmi_upper_bound,
                        "final_image_ids_2019": final_dry19.aggregate_array(
                            "system:index"
                        ),
                        "final_image_ids_2023": final_dry23.aggregate_array(
                            "system:index"
                        ),
                        "final_coverage_2019": cov19,
                        "final_coverage_2023": cov23,
                    }
                )
                .combine(
                    counts19.rename(
                        counts19.keys(),
                        counts19.keys().map(lambda k: ee.String(k).cat("_2019")),
                    )
                )
                .combine(
                    counts23.rename(
                        counts23.keys(),
                        counts23.keys().map(lambda k: ee.String(k).cat("_2023")),
                    )
                )
            )

            # Submit metadata export
            meta_task = ee.batch.Export.table.toCloudStorage(
                collection=ee.FeatureCollection(ee.Feature(None, metadata)),
                description=f"meta_{base_filename}",
                bucket=GCS_BUCKET,
                fileNamePrefix=f"metadata_v9/{VERSION_SUFFIX}/{base_filename}",
                fileFormat="GeoJSON",
            )
            meta_task.start()

            # Submit image export
            image_task = ee.batch.Export.image.toCloudStorage(
                image=pair_image,
                description=base_filename,
                bucket=GCS_BUCKET,
                fileNamePrefix=f"composites_v9/{VERSION_SUFFIX}/{base_filename}",
                region=geom,
                scale=PIX_SIZE,
                crs=CRS,
                maxPixels=1e13,
            )
            image_task.start()
            logging.info(
                f"SUBMITTED tasks for ID: {mpio_id_str} (Image: {image_task.id}, Meta: {meta_task.id})"
            )
        else:
            logging.info(f"Test mode: Exports not submitted for ID: {mpio_id_str}")

    except Exception as e:
        logging.error(
            f"!!! FAILED constructing graph for municipality ID {mpio_id_str}: {e}",
            exc_info=True,
        )
        pass


# --------------------------------------------------
# --- Main Execution Loop
# --------------------------------------------------
if __name__ == "__main__":
    if not os.path.exists(INPUT_LIST_FILE):
        logging.error(
            f"Input file not found: {INPUT_LIST_FILE}. Please run the List Generator or check the file path."
        )
    else:
        with open(INPUT_LIST_FILE, "r") as f:
            mpio_to_process = [line.strip() for line in f if line.strip()]

        logging.info(
            f"Starting FINAL Asynchronous Producer for {len(mpio_to_process)} municipalities from {INPUT_LIST_FILE}."
        )
        logging.info(
            f"Methodology: No Capping, q{CLEAR_THRESHOLD} Cloud Score, 30-70p NDMI Window"
        )
        logging.info(f"Output suffix: {VERSION_SUFFIX}")

        for i, munn_id in enumerate(mpio_to_process, 1):
            logging.info(f"===== Submitting Batch {i}/{len(mpio_to_process)} =====")
            process_municipality(munn_id)

        logging.info(
            "All Producer tasks for this stage have been submitted to Google Earth Engine."
        )
