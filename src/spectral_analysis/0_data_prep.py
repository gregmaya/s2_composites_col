import os

import numpy as np
import pandas as pd

# --- CONFIG ---
DATA_DIR = "data/results"
OUTPUT_DIR = "data/analysis"
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "master_analysis_data.csv")

# Files
FILE_URBAN = os.path.join(DATA_DIR, "urban_results.csv")
FILE_ROADS = os.path.join(DATA_DIR, "roads_results.csv")
FILE_RURAL = os.path.join(DATA_DIR, "rural_noroads_results.csv")
FILE_ROAD_AREAS = "data/road_areas_per_class.csv"


def load_and_prep():
    print("Loading datasets...")

    # 1. Load Urban
    df_urban = pd.read_csv(FILE_URBAN)
    df_urban["category"] = "Urban Center"
    df_urban["subtype"] = "Cabecera"
    df_urban["location"] = "Urban"

    # Normalize area column if present
    if "area_km2" in df_urban.columns:
        df_urban["area_km2"] = df_urban["area_km2"]
    else:
        df_urban["area_km2"] = np.nan

    # 2. Load Roads
    df_roads = pd.read_csv(FILE_ROADS)

    # --- DEDUPLICATE ---
    # Drop exact duplicates or duplicates based on ID+Subtype+Location
    df_roads.drop_duplicates(
        subset=["id", "subtype", "location"], keep="last", inplace=True
    )

    df_roads["category"] = "Roads"
    # Clean headers
    df_roads.columns = df_roads.columns.str.strip().str.lower()

    # --- JOIN ROAD AREAS ---
    if os.path.exists(FILE_ROAD_AREAS):
        print("Merging Road Areas...")
        df_areas = pd.read_csv(FILE_ROAD_AREAS, encoding="utf-8-sig")
        df_areas.columns = df_areas.columns.str.strip().str.lower()

        # Type cast
        df_roads["id"] = df_roads["id"].astype(str)
        df_areas["id"] = df_areas["id"].astype(str)

        # Merge
        df_merged_roads = pd.merge(
            df_roads,
            df_areas[["id", "class_re", "location", "area_km2"]],
            left_on=["id", "subtype", "location"],
            right_on=["id", "class_re", "location"],
            how="left",
        )

        # Rename columns if duplicates appeared (e.g. area_km2_x, area_km2_y)
        if "area_km2_y" in df_merged_roads.columns:
            df_merged_roads["area_km2"] = df_merged_roads["area_km2_y"]
        elif "area_km2_x" in df_merged_roads.columns:
            df_merged_roads["area_km2"] = df_merged_roads["area_km2_x"]

        df_roads = df_merged_roads
    else:
        print("WARNING: Road Areas file not found.")
        df_roads["area_km2"] = np.nan

    # 3. Load Rural
    df_rural = pd.read_csv(FILE_RURAL)
    df_rural["category"] = "Rural Background"
    df_rural["subtype"] = "No Roads"
    df_rural["location"] = "Rural"
    if "area_km2" in df_rural.columns:
        df_rural["area_km2"] = df_rural["area_km2"]
    else:
        df_rural["area_km2"] = np.nan

    # 4. MERGE ALL
    # Normalize all to lowercase first
    df_urban.columns = df_urban.columns.str.lower()
    df_roads.columns = df_roads.columns.str.lower()
    df_rural.columns = df_rural.columns.str.lower()

    # Define columns to keep (lowercase)
    common_cols = [
        "id",
        "category",
        "subtype",
        "location",
        "quality",
        "selection_reason",
        "area_km2",
    ]

    # Get index columns dynamically
    index_cols = [
        c
        for c in df_roads.columns
        if any(x in c for x in ["ndbi", "ndvi", "savi", "bsi", "ndmi", "ndwi"])
    ]

    cols_to_keep = common_cols + index_cols

    # Ensure columns exist
    for df in [df_urban, df_roads, df_rural]:
        for c in cols_to_keep:
            if c not in df.columns:
                df[c] = np.nan

    df_master = pd.concat(
        [df_urban[cols_to_keep], df_roads[cols_to_keep], df_rural[cols_to_keep]],
        ignore_index=True,
    )

    # 5. Calculate Impact
    if "delta_ndbi_mean" in df_master.columns:
        df_master["ndbi_impact"] = df_master["delta_ndbi_mean"] * df_master["area_km2"]

    # Final Lowercase Check
    df_master.columns = df_master.columns.str.lower()

    print(f"Merged Dataframe Shape: {df_master.shape}")
    print(f"Columns: {df_master.columns.tolist()}")
    df_master.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    load_and_prep()
