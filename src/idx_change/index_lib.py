import json

import ee

# Shared Config
MASTER_JSON_PATH = "data/master_composites_index_v3.json"
SCALE = 10


def init_ee():
    try:
        ee.Initialize()
    except Exception:
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
    """Adds NDBI, BSI, NDVI, SAVI, NDMI, NDWI, MNDWI, RI and their Deltas (23-19)."""

    def calc_year(suffix):
        b_blue = img.select(f"B2{suffix}")
        b_green = img.select(f"B3{suffix}")
        b_red = img.select(f"B4{suffix}")
        b_nir = img.select(f"B8{suffix}")
        b_swir1 = img.select(f"B11{suffix}")

        # NDBI
        ndbi = (
            b_swir1.subtract(b_nir).divide(b_swir1.add(b_nir)).rename(f"NDBI{suffix}")
        )

        # BSI
        bsi_top = (b_swir1.add(b_red)).subtract(b_nir.add(b_blue))
        bsi_bot = (b_swir1.add(b_red)).add(b_nir.add(b_blue))
        bsi = bsi_top.divide(bsi_bot).rename(f"BSI{suffix}")

        # NDVI
        ndvi = b_nir.subtract(b_red).divide(b_nir.add(b_red)).rename(f"NDVI{suffix}")

        # SAVI (L = 0.5)
        savi = (
            b_nir.subtract(b_red)
            .multiply(1.5)
            .divide(b_nir.add(b_red).add(0.5))
            .rename(f"SAVI{suffix}")
        )

        # NDMI
        ndmi = (
            b_nir.subtract(b_swir1).divide(b_nir.add(b_swir1)).rename(f"NDMI{suffix}")
        )

        # NDWI
        ndwi = (
            b_green.subtract(b_nir).divide(b_green.add(b_nir)).rename(f"NDWI{suffix}")
        )

        # MNDWI
        mndwi = (
            b_green.subtract(b_swir1)
            .divide(b_green.add(b_swir1))
            .rename(f"MNDWI{suffix}")
        )

        # Road Index (RI) – Reddy et al., Sentinel‑2 (Bands 11, 8, 2)
        sum_11_8_2 = b_swir1.add(b_nir).add(b_blue)
        min_11_8_2 = b_swir1.min(b_nir).min(b_blue)
        ri = (
            ee.Image(1)
            .subtract(min_11_8_2.multiply(3).divide(sum_11_8_2))
            .rename(f"RI{suffix}")
        )

        return [ndbi, bsi, ndvi, savi, ndmi, ndwi, mndwi, ri]

    idx_19 = calc_year("_2019")
    idx_23 = calc_year("_2023")

    # Add Deltas (23 - 19)
    bands = idx_19 + idx_23
    names = ["NDBI", "BSI", "NDVI", "SAVI", "NDMI", "NDWI", "MNDWI", "RI"]

    for i, name in enumerate(names):
        d = idx_23[i].subtract(idx_19[i]).rename(f"Delta_{name}")
        bands.append(d)

    return img.addBands(bands)


def compute_stats(img, geometry, reducer=None):
    """Computes Mean, StdDev, and Z-Scores for indices over a geometry."""
    if reducer is None:
        reducer = ee.Reducer.mean().combine(
            reducer2=ee.Reducer.stdDev(), sharedInputs=True
        )

    # Select all relevant bands (Raw Indices + Deltas)
    target_bands = img.select("ND.*|BSI.*|SAVI.*|MNDWI.*|RI.*|Delta.*")

    # Execute the reduction (Server-Side)
    stats = target_bands.reduceRegion(
        reducer=reducer,
        geometry=geometry,
        scale=SCALE,
        maxPixels=1e9,
        bestEffort=True,
    ).getInfo()

    # Post-processing: Calculate Z-Scores (Client-Side) for deltas
    indices = ["NDBI", "BSI", "NDVI", "SAVI", "NDMI", "NDWI", "MNDWI", "RI"]

    for idx in indices:
        mean_key = f"Delta_{idx}_mean"
        std_key = f"Delta_{idx}_stdDev"
        z_key = f"Delta_{idx}_z"

        if mean_key in stats and std_key in stats:
            mean = stats[mean_key]
            std = stats[std_key]
            stats[z_key] = (mean / std) if std and std > 0 else 0.0

    return stats
