import os
import sys

import ee

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import index_lib as lib

# Assets
# Pre-calculated: Municipality minus Urban Zones
ASSET_RURAL_NO_CABECERA = "projects/small-towns-col/assets/mun2018_nocabeceras_simpl10"
ASSET_ROADS_BASE = "projects/small-towns-col/assets/muni_road_geometries/municipality_"


def process(entry):
    mpio_id = entry["id"]

    # 1. Prepare Image
    img = lib.get_composite_image(entry)
    img = lib.add_indices_and_deltas(img)

    # 2. Get the Base Geometry (Rural without Cabecera)
    # Using robust filtering for ID string/int
    rural_fc = ee.FeatureCollection(ASSET_RURAL_NO_CABECERA).filter(
        ee.Filter.Or(
            ee.Filter.eq("MPIO_CCNCT", mpio_id),
            ee.Filter.eq("mpio_ccnct", int(mpio_id)),
        )
    )

    # If for some reason the geometry is missing (unlikely), return
    # Using .geometry() creates a single geometry from the collection
    rural_geom = rural_fc.geometry()

    # 3. Mask out the Roads
    # Instead of geometric difference (which crashes on complex road networks),
    # we use Raster Masking.
    road_asset_id = f"{ASSET_ROADS_BASE}{mpio_id}"

    try:
        roads = ee.FeatureCollection(road_asset_id)

        # Cheap check if roads exist using metadata if possible, or .limit(1)
        if roads.limit(1).size().getInfo() > 0:
            # A. Create a binary image where Roads = 1, Background = 0
            # We paint the road vectors onto a blank canvas
            road_pixels = ee.Image(0).byte().paint(roads, 1)

            # B. Invert it: Roads = 0, Background = 1
            # .Not() works on boolean/binary images
            rural_mask = road_pixels.Not()

            # C. Update the image mask
            # This hides any pixel that touches a road
            img = img.updateMask(rural_mask)
    except:
        # If road asset doesn't exist (e.g. Cumaribo), we simply don't mask them.
        # The Rural Background is just the No-Cabecera area.
        pass

    # 4. Compute Stats
    # We reduce the road-masked image over the No-Cabecera geometry
    stats = lib.compute_stats(img, rural_geom)

    if not stats or not stats.get("Delta_NDBI_mean"):
        return None

    row = {
        "id": mpio_id,
        "type": "Rural_Background",
        "subtype": "Rest of Muni (No Roads)",
        "location": "Rural",
        "quality": entry.get("selected_quality", "unknown"),
        "selection_reason": entry.get("selection_reason", "unknown"),
    }
    row.update(stats)

    return row
