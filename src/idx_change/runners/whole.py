import os
import sys

import ee

# Add the parent directory to path so we can import index_lib
# (Standard Python hack when running scripts inside subfolders)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import index_lib as lib

ASSET_BOUNDARIES = "projects/small-towns-col/assets/mun2018_simpl10"  # Add this


def process(entry):
    """
    Calculates stats for the entire municipality using the image footprint.
    """
    mpio_id = entry["id"]

    # 1. Get Image (Mosaic if tiled)
    img = lib.get_composite_image(entry)

    # 2. Add Indices & Deltas
    img = lib.add_indices_and_deltas(img)

    # 3. Define Geometry
    # NEW (Robust): Load geometry from vector asset
    # Filter strictly by ID
    geom = (
        ee.FeatureCollection(ASSET_BOUNDARIES)
        .filter(ee.Filter.eq("mpio_ccnct", int(mpio_id)))
        .geometry()
    )

    # 4. Compute Stats
    # We pass the geometry to the shared library function
    stats = lib.compute_stats(img, geom)

    # 5. Format Output
    if not stats:
        return None

    # Extract useful metadata from the Master JSON entry
    props = entry.get("properties", {})

    row = {
        "id": mpio_id,
        "type": "Whole_Muni",
        "quality": entry.get("selected_quality", "unknown"),
        "selection_reason": entry.get("selection_reason", "unknown"),
        "cov_19": props.get("final_coverage_2019", 0),
        "cov_23": props.get("final_coverage_2023", 0),
    }

    # Flatten the stats dictionary into the row
    row.update(stats)

    return row
