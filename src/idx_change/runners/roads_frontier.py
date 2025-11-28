import logging
import os
import sys

import ee

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import index_lib as lib

ASSET_ROADS_BASE = "projects/small-towns-col/assets/muni_road_geometries/municipality_"
ROAD_CLASSES = ["trunk", "primary", "secondary", "other"]
ZONES = [True, False]


def process(entry):
    mpio_id = entry["id"]
    logging.info(f"Frontier processing for {mpio_id}...")

    img = lib.get_composite_image(entry)
    img = lib.add_indices_and_deltas(img)

    asset_id = f"{ASSET_ROADS_BASE}{mpio_id}"

    try:
        roads = ee.FeatureCollection(asset_id)
        # Force a check to see if asset exists
        if roads.limit(1).size().getInfo() == 0:
            return []
    except:
        logging.warning(f"Asset not found for {mpio_id}")
        return []

    results = []

    for r_class in ROAD_CLASSES:
        for is_urban in ZONES:
            # Filter
            subset = roads.filter(
                ee.Filter.And(
                    ee.Filter.eq("class_re", r_class),
                    ee.Filter.eq("zona_urban", is_urban),
                )
            )

            # Cheap check
            if subset.limit(1).size().getInfo() == 0:
                continue

            try:
                # --- THE ROBUST METHOD ---
                # 1. Paint roads onto a blank canvas (1=Road, 0=Bg)
                road_mask = ee.Image(0).byte().paint(subset, 1)

                # 2. Mask the satellite image
                masked_img = img.updateMask(road_mask)

                # 3. Get bounding box (Cheap) instead of complex Union (Expensive)
                region = subset.geometry().bounds()

                # 4. Reduce
                stats = lib.compute_stats(masked_img, region)

                if stats and stats.get("Delta_NDBI_mean") is not None:
                    row = {
                        "id": mpio_id,
                        "type": "Roads",
                        "subtype": r_class,
                        "location": "Urban" if is_urban else "Rural",
                        "quality": entry.get("selected_quality", "unknown"),
                        "selection_reason": entry.get("selection_reason", "unknown"),
                    }
                    row.update(stats)
                    results.append(row)
                    logging.info(
                        f"  -> Success: {r_class} ({'Urban' if is_urban else 'Rural'})"
                    )

            except Exception as e:
                logging.warning(f"  -> Failed {r_class}: {e}")
                continue

    return results
