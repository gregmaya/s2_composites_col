import os
import sys

import ee

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import index_lib as lib

ASSET_ROADS_BASE = "projects/small-towns-col/assets/muni_road_geometries/municipality_"

# The specific classes you mentioned
ROAD_CLASSES = ["trunk", "primary", "secondary", "other"]
ZONES = [True, False]  # True=Urban, False=Rural


def process(entry):
    mpio_id = entry["id"]

    img = lib.get_composite_image(entry)
    img = lib.add_indices_and_deltas(img)

    asset_id = f"{ASSET_ROADS_BASE}{mpio_id}"

    # We assume the asset exists (the Main Runner handles asset-not-found errors)
    roads = ee.FeatureCollection(asset_id)

    results = []

    for r_class in ROAD_CLASSES:
        for is_urban in ZONES:
            # Filter: Class + Zone
            subset = roads.filter(
                ee.Filter.And(
                    ee.Filter.eq("class_re", r_class),
                    ee.Filter.eq("zona_urban", is_urban),
                )
            )

            # Get Geometry
            geom = subset.geometry()

            # Compute Stats
            # If geom is empty, compute_stats usually returns empty dict or None
            stats = lib.compute_stats(img, geom)

            # Validate Result
            # We check for a key indicator like 'NDBI_2019_mean' or 'Delta_NDBI_mean'
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

    return results
