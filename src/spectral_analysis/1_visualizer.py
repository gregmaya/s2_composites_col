# %% [markdown]
# # Spectral Analysis Visualization
# This script loads the master dataset and generates the key analysis plots.
# It is structured with `# %%` cells for interactive execution.

import os

import matplotlib.pyplot as plt

# %%
import pandas as pd
import seaborn as sns

# --- CONFIG ---
INPUT_FILE = "../../data/analysis/master_analysis_data_v2.csv"
PLOT_DIR = "src/spectral_analysis/plots"
os.makedirs(PLOT_DIR, exist_ok=True)

# Set Global Style
sns.set_theme(style="whitegrid")
plt.rcParams.update({"figure.figsize": (12, 8)})

# Load Data
if not os.path.exists(INPUT_FILE):
    print(f"Error: {INPUT_FILE} not found.")
else:
    df = pd.read_csv(INPUT_FILE)
    print(f"Loaded Data: {df.shape}")
    print(df.head())


# %%
# Check unique locations
print("Locations:", df["location"].unique())

# Check Road Subtypes
print("Road Subtypes:", df[df["category"] == "Roads"]["subtype"].unique())

# Check Missing Areas
missing_area = df[df["category"] == "Roads"]["area_km2"].isnull().sum()
total_roads = len(df[df["category"] == "Roads"])
print(
    f"Missing Areas in Roads: {missing_area} / {total_roads} ({missing_area / total_roads:.1%})"
)

# Sample check for one of the 'problem' IDs (e.g. 99773)
print("\nCheck for ID 99773:")
print(
    df[df["id"] == 99773][
        ["category", "subtype", "location", "area_km2", "delta_ndbi_mean"]
    ]
)


# %% [markdown]
# ## Investigation 1: Data Integrity Check
# Before plotting, let's verify the join and data completeness.


# %%
def plot_road_area_distribution(df):
    print("Generating: Road Area Distribution...")

    df_roads = df[df["category"] == "Roads"].copy()

    # Aggregate Area by Subtype and Location
    area_stats = (
        df_roads.groupby(["subtype", "location"])["area_km2"].sum().reset_index()
    )

    # Sort order (same as other charts)
    order = ["trunk", "primary", "secondary", "other"]

    plt.figure(figsize=(10, 6))

    # Horizontal Bar Plot
    ax = sns.barplot(
        data=area_stats,
        y="subtype",
        x="area_km2",
        hue="location",
        order=order,
        palette={"Urban": "#3498db", "Rural": "#e74c3c"},
        orient="h",
    )

    plt.title("Total Road Surface Area by Class and Context", fontsize=16)
    plt.xlabel("Total Area (km²)", fontsize=12)
    plt.ylabel("Road Class", fontsize=12)
    plt.legend(title="Context")

    # Add labels to the end of bars
    for i in ax.containers:
        ax.bar_label(i, fmt="%.0f", padding=3)

    plt.tight_layout()
    plt.savefig(f"{PLOT_DIR}/0_road_area_distribution.png", dpi=300)
    plt.show()


plot_road_area_distribution(df)

# %%# %%
# --- CLEAN TABLE GENERATION ---
df_roads = df[df["category"] == "Roads"].copy()

summary = df_roads.groupby("subtype").agg(
    Munis_Present=("id", "nunique"), Total_Area_km2=("area_km2", "sum")
)

# Add Percentage columns
summary["Presence (%)"] = (summary["Munis_Present"] / 698 * 100).map("{:.1f}%".format)
summary["Area Share (%)"] = (
    summary["Total_Area_km2"] / summary["Total_Area_km2"].sum() * 100
).map("{:.1f}%".format)

# Format Area
summary["Total_Area_km2"] = summary["Total_Area_km2"].map("{:,.0f}".format)

# Reorder and Rename for Report
summary = summary.reindex(["trunk", "primary", "secondary", "other"])
summary.index.name = "Road Class"
summary.columns = [
    "Municipalities (Count)",
    "Total Surface Area (km²)",
    "Presence in Municipalities (%)",
    "Area Share (%)",
]

print(summary)
# To export for Excel/Word:
# summary.to_csv("data/analysis/road_network_summary.csv")


# %%
def plot_rural_vs_urban_roads(df):
    index_delta_col = "delta_ri_mean"
    print("Generating: Rural vs Urban Roads Comparison...")

    df_roads = df[df["category"] == "Roads"].copy()
    order = ["trunk", "primary", "secondary", "other"]

    plt.figure(figsize=(10, 6))

    ax = sns.barplot(
        data=df_roads,
        x="subtype",
        y=index_delta_col,
        hue="location",
        order=order,
        palette={"Urban": "#3498db", "Rural": "#e74c3c"},
        errorbar=("ci", 95),
        capsize=0.1,
    )

    plt.title("Magnitude of Change: Urban vs Rural Roads", fontsize=16)
    plt.ylabel(
        f"Average {index_delta_col.replace('_', ' ').title()}  (2019-2023)", fontsize=12
    )
    plt.xlabel("Road Class", fontsize=12)
    plt.axhline(0, color="black", linewidth=0.8, linestyle="--")

    # --- ADD LABELS ---
    # Loop through the containers (groups of bars)
    # i is the container index (0 = Urban bars, 1 = Rural bars)
    for i in ax.containers:
        # ax.bar_label handles the positioning automatically
        # fmt='%.3f' gives 3 decimal places (e.g., 0.014)
        # padding=3 moves the text slightly up (or down if negative)
        # We might need to skip labels for Error Bars (lines), but containers usually hold the rectangles.
        ax.bar_label(i, fmt="%.3f", padding=3, fontsize=10)

    plt.tight_layout()
    plt.savefig(f"{PLOT_DIR}/1_rural_vs_urban_roads.png", dpi=300)
    plt.show()


plot_rural_vs_urban_roads(df)


# %% [markdown]
# ## Chart 2: Weighted Impact Analysis (Mass)
# Magnitude is good, but Impact (Magnitude * Area) tells the real story.


# %%
def plot_weighted_impact(df):
    print("Generating: Weighted Impact Analysis...")

    df_roads = df[df["category"] == "Roads"].copy()

    # Aggregate sum of impact
    impact_stats = (
        df_roads.groupby(["location", "subtype"])["ndbi_impact"].sum().reset_index()
    )

    # Investigation: Print the stats table
    print("\nImpact Stats Table:")
    print(impact_stats)

    plt.figure(figsize=(10, 6))

    ax = sns.barplot(
        data=impact_stats,
        x="subtype",
        y="ndbi_impact",
        hue="location",
        order=["primary", "secondary", "other", "trunk"],
        palette={"Urban": "#3498db", "Rural": "#e74c3c"},
    )

    plt.title("Total Weighted Impact of Change (Magnitude × Area)", fontsize=16)
    plt.ylabel("Cumulative Impact (Delta-km²)", fontsize=12)
    plt.xlabel("Road Class", fontsize=12)

    for i in ax.containers:
        ax.bar_label(i, fmt="%.1f", padding=3)

    plt.tight_layout()
    plt.savefig(f"{PLOT_DIR}/2_weighted_impact.png", dpi=300)
    plt.show()


plot_weighted_impact(df)

# %% [markdown]
# ## Chart 3: Control Group Validation
# Is the change specific to roads, or is the whole rural area changing?


def plot_detailed_validation(df):
    indexcol = "delta_ndbi_mean"
    print("Generating: Detailed Validation by Subtype...")

    # Filter for Rural only
    df_rural = df[df["location"] == "Rural"].copy()

    # Create a new 'Comparison Class' column
    # If Category is Rural Background -> 'Background'
    # If Category is Roads -> use Subtype (e.g. 'primary', 'other')

    def get_class(row):
        if row["category"] == "Rural Background":
            return "Background"
        else:
            return row["subtype"]  # e.g. 'primary', 'other'

    df_rural["Comparison_Class"] = df_rural.apply(get_class, axis=1)

    # Define Order: Roads first, then Background
    order = ["trunk", "primary", "secondary", "other", "Background"]

    plt.figure(figsize=(12, 7))

    # Boxplot
    sns.boxplot(
        data=df_rural,
        x="Comparison_Class",
        y=indexcol,
        order=order,
        palette={
            "trunk": "#e74c3c",
            "primary": "#e74c3c",
            "secondary": "#e74c3c",
            "other": "#e74c3c",
            "Background": "#95a5a6",  # Grey for background
        },
        showfliers=False,
    )

    plt.title(
        "Detailed Validation: Rural Road Subtypes vs. Background Stability", fontsize=16
    )
    plt.ylabel(
        f"{str.capitalize(str.replace(indexcol, '_', ' '))} (2019-2023)", fontsize=12
    )
    plt.xlabel("Rural Class", fontsize=12)
    plt.axhline(0, color="black", linewidth=0.8, linestyle="--")

    plt.tight_layout()
    # plt.savefig(f"{PLOT_DIR}/5_detailed_validation.png", dpi=300)
    plt.show()


plot_detailed_validation(df)


# %%# %% [markdown]
# ## Chart 4: The Paving Process (Scatter)
# Looking for the "Dirt to Paved" signature.


# %%
def plot_paving_scatter(df):
    print("Generating: Paving Process Scatter...")

    # Filter: Rural Roads only, exclude Trunk if desired
    df_roads_rural = df[
        (df["category"] == "Roads") & (df["location"] == "Rural")
    ].copy()

    plt.figure(figsize=(10, 8))

    sns.scatterplot(
        data=df_roads_rural,
        x="ri_2019_mean",
        y="delta_ri_mean",
        hue="subtype",
        alpha=0.6,
        style="subtype",
    )

    plt.title(
        "The Paving Signature: Initial State vs Change (Rural Roads)", fontsize=16
    )
    plt.xlabel("Initial RI (2019) [Lower = Green/Dirt]", fontsize=12)
    plt.ylabel("Change in RI [Higher = Paving]", fontsize=12)
    plt.axhline(0, color="black", linewidth=0.5)
    plt.axvline(0, color="black", linewidth=0.5)

    plt.tight_layout()
    plt.savefig(f"{PLOT_DIR}/4_paving_scatter.png", dpi=300)
    plt.show()


plot_paving_scatter(df)


# %%
def plot_kde_facets(df):
    indexcol_y = "delta_ri_mean"
    indexcol_x = "ri_2019_mean"
    print("Generating: Faceted KDE Paving Signature...")

    # Filter: Rural Roads only
    df_rural = df[(df["category"] == "Roads") & (df["location"] == "Urban")].copy()

    # Set up the FacetGrid
    g = sns.FacetGrid(
        df_rural,
        col="subtype",
        col_wrap=2,
        hue="subtype",
        sharex=True,
        sharey=True,
        height=4,
        aspect=1.2,
        col_order=["trunk", "primary", "secondary", "other"],
        palette={
            "trunk": "#e74c3c",
            "primary": "#e67e22",
            "secondary": "#f1c40f",
            "other": "#2ecc71",
        },
    )

    # Draw Density Contours (filled)
    g.map_dataframe(
        sns.kdeplot,
        x=indexcol_x,
        y=indexcol_y,
        fill=True,
        alpha=0.6,
        levels=10,
        thresh=0.05,  # Hide lowest density noise
    )

    # Add Reference Lines to each subplot
    def add_refs(**kwargs):
        plt.axhline(0, color="black", linewidth=0.8, linestyle="--")
        plt.axvline(0, color="black", linewidth=0.8, linestyle="--")

    g.map(add_refs)

    # Titles and Labels
    g.set_titles("{col_name} Roads")
    g.set_axis_labels("Initial NDBI (2019)", "Change in NDBI (2019-2023)")

    # Adjust Layout
    plt.subplots_adjust(top=0.9)
    g.fig.suptitle("Spectral Migration by Road Class (Urban Context)", fontsize=16)

    plt.savefig(f"{PLOT_DIR}/6_kde_facets.png", dpi=300)
    plt.show()


plot_kde_facets(df)


# %%
def plot_kde_other_context(df):
    print("Generating: KDE - Tertiary Roads (Urban vs Rural)...")

    # Filter: 'Other' roads only
    df_other = df[(df["category"] == "Roads") & (df["subtype"] == "other")].copy()

    plt.figure(figsize=(10, 8))

    # Plot Urban (Blue)
    sns.kdeplot(
        data=df_other[df_other["location"] == "Urban"],
        x="ndbi_2019_mean",
        y="delta_ndbi_mean",
        fill=True,
        alpha=0.5,
        color="#3498db",
        label="Urban Context",
    )

    # Plot Rural (Red)
    sns.kdeplot(
        data=df_other[df_other["location"] == "Rural"],
        x="ndbi_2019_mean",
        y="delta_ndbi_mean",
        fill=True,
        alpha=0.5,
        color="#e74c3c",
        label="Rural Context",
    )

    # Reference Lines
    plt.axhline(0, color="black", linewidth=0.8, linestyle="--")
    plt.axvline(0, color="black", linewidth=0.8, linestyle="--")

    plt.title(
        "Spectral Migration of Tertiary ('Other') Roads: Urban vs. Rural", fontsize=16
    )
    plt.xlabel("Initial NDBI (2019)", fontsize=12)
    plt.ylabel("Change in NDBI (2019-2023)", fontsize=12)
    plt.legend()

    plt.tight_layout()
    plt.savefig(f"{PLOT_DIR}/10_kde_other_context.png", dpi=300)
    plt.show()


plot_kde_other_context(df)


# %%
def plot_correlation_profile(df):
    print("Generating: Correlation Profile Comparison...")

    df_rural = df[(df["category"] == "Roads") & (df["location"] == "Rural")].copy()

    # Define the pairs we care about
    # Pair 1: NDBI vs NDVI (Should be Negative)
    # Pair 2: NDBI vs BSI (Should be Positive)
    # Pair 3: NDBI vs SAVI (Should be Negative)

    results = []
    for subtype in ["trunk", "primary", "secondary", "other"]:
        data = df_rural[df_rural["subtype"] == subtype]

        corr_ndvi = data["delta_ndbi_mean"].corr(data["delta_ndvi_mean"])
        corr_ri = data["delta_ndbi_mean"].corr(data["delta_ri_mean"])
        corr_savi = data["delta_ndbi_mean"].corr(data["delta_savi_mean"])

        results.append(
            {"Subtype": subtype, "Pair": "NDBI vs NDVI", "Correlation": corr_ndvi}
        )
        results.append(
            {"Subtype": subtype, "Pair": "NDBI vs RI", "Correlation": corr_ri}
        )
        results.append(
            {"Subtype": subtype, "Pair": "NDBI vs SAVI", "Correlation": corr_savi}
        )

    df_corr = pd.DataFrame(results)

    plt.figure(figsize=(10, 6))

    sns.barplot(
        data=df_corr,
        x="Pair",
        y="Correlation",
        hue="Subtype",
        palette={
            "trunk": "#e74c3c",
            "primary": "#e67e22",
            "secondary": "#f1c40f",
            "other": "#2ecc71",
        },
    )

    plt.title(
        "Spectral Consistency Check: Correlations across Road Classes in Rural Context",
        fontsize=16,
    )
    plt.ylabel("Pearson Correlation Coefficient", fontsize=12)
    plt.xlabel("Index Relationship", fontsize=12)
    plt.axhline(0, color="black", linewidth=0.8)
    plt.ylim(-1, 1)

    plt.tight_layout()
    plt.savefig(f"{PLOT_DIR}/9_correlation_profile.png", dpi=300)
    plt.show()


plot_correlation_profile(df)


# %%
def plot_savi_ndbi_scatter(df):
    area = "Urban"

    print("Generating: RI vs SAVI Mechanism Check...")

    # Filter: Urban Roads only
    df_temp = df[(df["category"] == "Roads") & (df["location"] == area)].copy()

    # Set up the FacetGrid
    g = sns.FacetGrid(
        df_temp,
        col="subtype",
        col_wrap=2,
        hue="subtype",
        sharex=True,
        sharey=True,
        height=4,
        aspect=1.2,
        col_order=["trunk", "primary", "secondary", "other"],
        palette={
            "trunk": "#e74c3c",
            "primary": "#e67e22",
            "secondary": "#f1c40f",
            "other": "#2ecc71",
        },
    )

    # Draw Scatter points
    g.map(sns.scatterplot, "delta_ri_mean", "delta_savi_mean", alpha=0.6, s=30)

    # Draw Regression Line (to show the trend clearly)
    g.map(
        sns.regplot,
        "delta_ri_mean",
        "delta_savi_mean",
        scatter=False,
        ci=None,
        line_kws={"color": "black", "linewidth": 1.5, "linestyle": "--"},
    )

    # Reference Lines
    def add_refs(**kwargs):
        plt.axhline(0, color="gray", linewidth=0.5)
        plt.axvline(0, color="gray", linewidth=0.5)

    g.map(add_refs)

    # Titles and Labels
    g.set_titles("{col_name} Roads")
    g.set_axis_labels("Change in RI", "Change in SAVI (Vegetation)")

    # Adjust Layout
    plt.subplots_adjust(top=0.9)
    g.fig.suptitle(
        f"Mechanism Check: Hardening vs. Vegetation Loss ({area})", fontsize=16
    )

    # plt.savefig(f"{PLOT_DIR}/11_savi_ndbi_scatter.png", dpi=300)
    plt.show()


plot_savi_ndbi_scatter(df)

# %%
