import os
import sys

import ee

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import index_lib as lib

ASSET_URBAN = "projects/small-towns-col/assets/col_zon_urb_sel"


def process(entry):
    """
    Calculates stats for the Urban Zone (Cabecera) only.
    Filters: clas_ccdgo='1' (Cabecera) AND selected_m=true
    """
    mpio_id = entry["id"]

    img = lib.get_composite_image(entry)
    img = lib.add_indices_and_deltas(img)

    # Strict Geometry Filter
    geom = (
        ee.FeatureCollection(ASSET_URBAN)
        .filter(
            ee.Filter.eq("mpio_cdpmp", int(mpio_id))
        )  # different label than the one in other datasets
        .filter(ee.Filter.eq("clas_ccdgo", "1"))
        .geometry()
    )

    # Compute
    stats = lib.compute_stats(img, geom)

    if not stats or not stats.get("Delta_NDBI_mean"):
        return None  # No Cabecera found for this ID

    row = {
        "id": mpio_id,
        "type": "Urban_Zone",
        "quality": entry.get("selected_quality", "unknown"),
        "selection_reason": entry.get("selection_reason", "unknown"),
    }
    row.update(stats)

    return row
