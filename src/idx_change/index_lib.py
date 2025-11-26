import json

import ee

# Shared Config
MASTER_JSON_PATH = "data/master_composites_index_v3.json"
SCALE = 10


def init_ee():
    try:
        ee.Initialize()
    except:
        ee.Authenticate()
        ee.Initialize()


def load_master_json():
    with open(MASTER_JSON_PATH, "r") as f:
        return json.load(f)


def get_composite_image(entry):
    """Loads and mosaics the composite for a given JSON entry."""
    urls = entry["image_files"]
    if len(urls) == 1:
        img = ee.Image.loadGeoTIFF(urls[0])
    else:
        img = ee.ImageCollection([ee.Image.loadGeoTIFF(u) for u in urls]).mosaic()
    return img


def add_indices_and_deltas(img):
    """Adds NDBI, BSI, NDVI... and their Deltas (23-19)."""

    def calc_year(suffix):
        b_blue = img.select(f"B2{suffix}")
        b_green = img.select(f"B3{suffix}")
        b_red = img.select(f"B4{suffix}")
        b_nir = img.select(f"B8{suffix}")
        b_swir1 = img.select(f"B11{suffix}")

        ndbi = (
            b_swir1.subtract(b_nir).divide(b_swir1.add(b_nir)).rename(f"NDBI{suffix}")
        )

        # BSI formula
        bsi_top = (b_swir1.add(b_red)).subtract(b_nir.add(b_blue))
        bsi_bot = (b_swir1.add(b_red)).add(b_nir.add(b_blue))
        bsi = bsi_top.divide(bsi_bot).rename(f"BSI{suffix}")

        ndvi = b_nir.subtract(b_red).divide(b_nir.add(b_red)).rename(f"NDVI{suffix}")
        savi = (
            b_nir.subtract(b_red)
            .multiply(1.5)
            .divide(b_nir.add(b_red).add(0.5))
            .rename(f"SAVI{suffix}")
        )
        ndmi = (
            b_nir.subtract(b_swir1).divide(b_nir.add(b_swir1)).rename(f"NDMI{suffix}")
        )
        ndwi = (
            b_green.subtract(b_nir).divide(b_green.add(b_nir)).rename(f"NDWI{suffix}")
        )
        mndwi = (
            b_green.subtract(b_swir1)
            .divide(b_green.add(b_swir1))
            .rename(f"MNDWI{suffix}")
        )

        return [ndbi, bsi, ndvi, savi, ndmi, ndwi, mndwi]

    idx_19 = calc_year("_2019")
    idx_23 = calc_year("_2023")

    # Add Deltas
    bands = idx_19 + idx_23
    names = ["NDBI", "BSI", "NDVI", "SAVI", "NDMI", "NDWI", "MNDWI"]
    for i in range(len(names)):
        d = idx_23[i].subtract(idx_19[i]).rename(f"Delta_{names[i]}")
        bands.append(d)

    return img.addBands(bands)


def compute_stats(img, geometry, reducer=None):
    """Computes Mean, StdDev, and Z-Scores for indices over a geometry."""
    if reducer is None:
        reducer = ee.Reducer.mean().combine(
            reducer2=ee.Reducer.stdDev(), sharedInputs=True
        )

    # Select all relevant bands (Raw Indices + Deltas)
    target_bands = img.select("ND.*|BSI.*|SAVI.*|MNDWI.*|Delta.*")

    # Execute the reduction (Server-Side)
    stats = target_bands.reduceRegion(
        reducer=reducer, geometry=geometry, scale=SCALE, maxPixels=1e9, bestEffort=True
    ).getInfo()

    # Post-processing: Calculate Z-Scores (Client-Side)
    indices = ["NDBI", "BSI", "NDVI", "SAVI", "NDMI", "NDWI", "MNDWI"]

    for idx in indices:
        mean_key = f"Delta_{idx}_mean"
        std_key = f"Delta_{idx}_stdDev"
        z_key = f"Delta_{idx}_z"

        # Only calc if both keys exist (safety check)
        if mean_key in stats and std_key in stats:
            mean = stats[mean_key]
            std = stats[std_key]

            # Avoid division by zero
            if std and std > 0:
                stats[z_key] = mean / std
            else:
                stats[z_key] = 0.0

    return stats
