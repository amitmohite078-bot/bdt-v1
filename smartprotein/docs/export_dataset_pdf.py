"""Export SmartProtein Dataset Overview, Schemas & Sample Records to a Multi-Page PDF.

Uses Pillow + Pantone Option 1 ("Clinical Nutrition & Trust") palette to generate:
  smartprotein/docs/SmartProtein_Dataset_Report.pdf
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from PIL import Image, ImageDraw, ImageFont

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
CURATED_DIR = DATA_DIR / "curated"
REJECTED_DIR = DATA_DIR / "rejected" / "all_records"
ANALYTICS_DIR = DATA_DIR / "analytics"
OUT_PDF = ROOT_DIR / "docs" / "SmartProtein_Dataset_Report.pdf"

# A4 Landscape @ 150 DPI
PAGE_W, PAGE_H = 1754, 1240

# Pantone Option 1 Palette
CLASSIC_BLUE = "#0F4C81"
EMERALD = "#009473"
CLOUD_DANCER = "#F0EEE9"
ANTHRACITE = "#28282D"
LIVING_CORAL = "#FF6F61"
GREENERY = "#88B04B"
WHITE = "#FFFFFF"
BORDER_GRAY = "#D5D2CA"


def get_font(size: int) -> ImageFont.ImageFont:
    for candidate in [
        Path("C:/Windows/Fonts/segoeui.ttf"),
        Path("C:/Windows/Fonts/arial.ttf"),
        Path("C:/Windows/Fonts/calibri.ttf"),
    ]:
        if candidate.exists():
            try:
                return ImageFont.truetype(str(candidate), size=size)
            except Exception:
                pass
    return ImageFont.load_default(size=size)


def new_page(title: str, subtitle: str, page_num: int, total_pages: int) -> tuple[Image.Image, ImageDraw.ImageDraw]:
    img = Image.new("RGB", (PAGE_W, PAGE_H), CLOUD_DANCER)
    draw = ImageDraw.Draw(img)

    # Top header banner (Pantone Classic Blue + Emerald accent bar)
    draw.rectangle([(0, 0), (PAGE_W, 118)], fill=CLASSIC_BLUE)
    draw.rectangle([(0, 118), (PAGE_W, 126)], fill=EMERALD)

    f_title = get_font(34)
    f_sub = get_font(19)
    f_foot = get_font(16)

    draw.text((48, 24), title, fill=WHITE, font=f_title)
    draw.text((48, 74), subtitle, fill="#DCE8F2", font=f_sub)

    # Footer
    draw.line([(48, PAGE_H - 52), (PAGE_W - 48, PAGE_H - 52)], fill=BORDER_GRAY, width=2)
    draw.text(
        (48, PAGE_H - 40),
        "SmartProtein: Hadoop HDFS Big Data Quality & Accurate Protein Pipeline",
        fill=ANTHRACITE,
        font=f_foot,
    )
    draw.text(
        (PAGE_W - 180, PAGE_H - 40),
        f"Page {page_num} / {total_pages}",
        fill=CLASSIC_BLUE,
        font=f_foot,
    )
    return img, draw


def draw_table(
    draw: ImageDraw.ImageDraw,
    df: pd.DataFrame,
    x0: int,
    y0: int,
    col_widths: list[int],
    row_h: int = 40,
    header_color: str = CLASSIC_BLUE,
) -> int:
    f_hdr = get_font(17)
    f_cell = get_font(15)

    # Header row
    cur_x = x0
    total_w = sum(col_widths)
    draw.rectangle([(x0, y0), (x0 + total_w, y0 + row_h + 6)], fill=header_color)
    for col_name, w in zip(df.columns, col_widths):
        label = str(col_name)[: max(6, w // 10)]
        draw.text((cur_x + 10, y0 + 12), label, fill=WHITE, font=f_hdr)
        cur_x += w

    y = y0 + row_h + 6
    for r_idx, (_, row) in enumerate(df.iterrows()):
        bg = WHITE if r_idx % 2 == 0 else "#F8F7F4"
        draw.rectangle([(x0, y), (x0 + total_w, y + row_h)], fill=bg, outline=BORDER_GRAY)
        cur_x = x0
        for col_name, w in zip(df.columns, col_widths):
            val = str(row[col_name])
            max_chars = max(5, (w - 16) // 9)
            if len(val) > max_chars:
                val = val[: max_chars - 2] + ".."
            draw.text((cur_x + 10, y + 10), val, fill=ANTHRACITE, font=f_cell)
            cur_x += w
        y += row_h
    return y


def main() -> None:
    # Load actual live datasets from smartprotein/data/
    sc_df = pd.read_parquet(ANALYTICS_DIR / "dq_scorecards.snappy.parquet")
    cost_cmp = pd.read_parquet(ANALYTICS_DIR / "usable_protein_cost_comparison.snappy.parquet")
    rej_df = pd.read_parquet(REJECTED_DIR / "rejected_records.snappy.parquet")
    lin_df = pd.read_parquet(ANALYTICS_DIR / "dq_lineage.snappy.parquet")
    inv_df = pd.read_parquet(CURATED_DIR / "local_store_inventory.snappy.parquet")
    recs_df = pd.read_parquet(ANALYTICS_DIR / "gold_user_recommendations.snappy.parquet")
    stores_df = pd.read_csv(RAW_DIR / "store_locations" / "store_locations.csv")

    fc_files = sorted((RAW_DIR / "food_composition").rglob("food_composition_raw.csv"))
    ocr_files = sorted((RAW_DIR / "food_composition").rglob("ocr_labels_raw.csv"))
    raw_fc = pd.read_csv(fc_files[-1])
    raw_ocr = pd.read_csv(ocr_files[-1]) if ocr_files else pd.DataFrame()
    raw_sp_files = sorted((RAW_DIR / "store_prices").rglob("store_prices_raw.csv"))
    raw_uw_files = sorted((RAW_DIR / "user_wearables").rglob("user_wearables_raw.csv"))
    raw_sp = pd.read_csv(raw_sp_files[-1])
    raw_uw = pd.read_csv(raw_uw_files[-1])

    total_pages = 6
    pages: list[Image.Image] = []
    f_sec = get_font(22)
    f_note = get_font(17)

    # ------------------------------------------------------------------
    # PAGE 1: OVERVIEW & BEFORE/AFTER DQ SUMMARY
    # ------------------------------------------------------------------
    p1, d1 = new_page(
        "SmartProtein Dataset Report & Schema Overview",
        "Multi-Source Raw Inputs, Curated Accurate Protein Tables, Quarantine & Gold Outputs",
        1,
        total_pages,
    )
    d1.text((48, 150), "1. Raw & Curated Dataset Inventory (Latest Batch On Disk)", fill=CLASSIC_BLUE, font=f_sec)
    inv_summary = pd.DataFrame(
        [
            {"Zone / Table": "Raw: food_composition_raw.csv", "Rows (Latest File)": f"{len(raw_fc):,}", "Format": "CSV", "Key Purpose": "USDA_FDC, IFCT_2017 & OPEN_FOOD_FACTS w/ ~18% injected faults"},
            {"Zone / Table": "Raw: ocr_labels_raw.csv", "Rows (Latest File)": f"{len(raw_ocr):,}", "Format": "CSV + PNG", "Key Purpose": "Parsed nutrition labels (valid Paneer + corrupt 135g Soy)"},
            {"Zone / Table": "Raw: store_locations.csv", "Rows (Latest File)": f"{len(stores_df):,}", "Format": "CSV", "Key Purpose": "400 stores across 4 regions w/ Lat, Long & GeoHash-6"},
            {"Zone / Table": "Raw: store_prices_raw.csv", "Rows (Latest File)": f"{len(raw_sp):,}", "Format": "CSV", "Key Purpose": "Pack weights, INR/USD prices, stock status w/ negative errors"},
            {"Zone / Table": "Raw: user_wearables_raw.csv", "Rows (Latest File)": f"{len(raw_uw):,}", "Format": "CSV", "Key Purpose": "Biometrics, diet, allergies, budget, steps, workout & sleep"},
            {"Zone / Table": "Curated: protein_quality_enriched", "Rows (Latest File)": "80", "Format": "Snappy Parquet", "Key Purpose": "DIAAS, cooking, phytate bioavailability, leucine & usable protein"},
            {"Zone / Table": "Analytics: gold_user_recommendations", "Rows (Latest File)": f"{len(recs_df):,}", "Format": "Snappy Parquet", "Key Purpose": "Explainable Top-5 recommendations (250 user cohort)"},
        ]
    )
    y_end = draw_table(d1, inv_summary, 48, 188, [380, 190, 160, 920], row_h=42, header_color=CLASSIC_BLUE)

    d1.text((48, y_end + 32), "2. Data Quality Scorecard: Before vs. After Cleaning (0-100)", fill=EMERALD, font=f_sec)
    sc_show = sc_df[["dataset_name", "stage", "total_records", "clean_records", "rejected_records", "completeness_score", "validity_score", "consistency_score", "composite_dq_score"]].copy()
    draw_table(d1, sc_show, 48, y_end + 70, [210, 190, 150, 150, 160, 175, 160, 175, 210], row_h=42, header_color=EMERALD)
    pages.append(p1)

    # ------------------------------------------------------------------
    # PAGE 2: SAMPLE RAW DIRTY FOOD COMPOSITION RECORDS
    # ------------------------------------------------------------------
    p2, d2 = new_page(
        "Sample Raw Dirty Data: food_composition_raw.csv + OCR Labels",
        "Illustrating Deliberate Big Data Defects (kJ swaps, >100g protein, Vegan meat lies, typos, nulls)",
        2,
        total_pages,
    )
    d2.text((48, 148), "Sample Clean & Corrupted Rows Entering Zone 1 (/raw/food_composition)", fill=CLASSIC_BLUE, font=f_sec)
    # Pick diverse examples: clean, null, >100g, kJ, Vegan chicken/paneer, OCR
    dirty_examples = pd.concat(
        [
            raw_fc[raw_fc["protein_g"] > 100].head(3),
            raw_fc[raw_fc["energy_unit"] == "kJ"].head(3),
            raw_fc[(raw_fc["diet_type"] == "Vegan") & (raw_fc["food_name"].fillna("").str.contains("Chicken|Paneer|Egg", case=False))].head(3),
            raw_fc[raw_fc["serving_unit"] == "per_50g_serving"].head(3),
            raw_fc[raw_fc["protein_g"].isna()].head(2),
            raw_ocr.head(2),
            raw_fc.head(3),
        ],
        ignore_index=True,
    )[["record_id", "source_db", "food_name", "diet_type", "serving_unit", "protein_g", "carbs_g", "fat_g", "reported_kcal", "energy_unit"]]
    draw_table(d2, dirty_examples, 48, 188, [145, 175, 310, 125, 185, 125, 120, 110, 165, 125], row_h=44, header_color=CLASSIC_BLUE)
    pages.append(p2)

    # ------------------------------------------------------------------
    # PAGE 3: CURATED ACCURATE PROTEIN COMPARISON TABLE
    # ------------------------------------------------------------------
    p3, d3 = new_page(
        "Curated Accurate Protein Table (DIAAS, Bioavailability & Usable Cost)",
        "Zone 3 (/curated/protein_quality_enriched) Joined with Regional Store Median Prices",
        3,
        total_pages,
    )
    d3.text((48, 148), "Crude vs. Usable Protein, Cost per Gram of Usable Protein & 2.5g Leucine Portion", fill=EMERALD, font=f_sec)
    cmp_show = cost_cmp[
        [
            "canonical_name", "diet_type", "diaas_score", "bioavailability_factor",
            "crude_protein_per_100g", "usable_protein_per_100g", "median_price_100g_inr",
            "cost_per_g_usable_protein_inr", "grams_for_2_5g_leucine", "protein_confidence_score",
        ]
    ].head(19).copy()
    draw_table(d3, cmp_show, 48, 188, [265, 120, 115, 155, 170, 175, 165, 175, 160, 150], row_h=43, header_color=EMERALD)
    pages.append(p3)

    # ------------------------------------------------------------------
    # PAGE 4: QUARANTINE (REJECTED) & DATA LINEAGE SAMPLES
    # ------------------------------------------------------------------
    p4, d4 = new_page(
        "Zone 4 Quarantine (/rejected) & Zone 5 Data Lineage (/analytics)",
        "Every Failed Record is Tagged with Reject Reason Codes & Full Provenance Instead of Silent Deletion",
        4,
        total_pages,
    )
    d4.text((48, 146), "Sample Quarantined Records Across Multi-Modal Feeds", fill=LIVING_CORAL, font=f_sec)
    rej_sample = rej_df.groupby("reject_reason_code", as_index=False).head(1).head(10)[
        ["source_dataset", "raw_record_id", "reject_reason_code", "detected_by", "ingestion_date"]
    ]
    y_r = draw_table(d4, rej_sample, 48, 182, [280, 260, 420, 260, 260], row_h=40, header_color=LIVING_CORAL)

    d4.text((48, y_r + 28), "Sample Row-Level Data Lineage (raw -> rule -> target_zone)", fill=CLASSIC_BLUE, font=f_sec)
    lin_sample = lin_df.head(7)[["lineage_id", "raw_source_file", "raw_record_id", "rule_applied", "action_taken", "target_record_id"]]
    draw_table(d4, lin_sample, 48, y_r + 66, [160, 320, 180, 420, 280, 220], row_h=40, header_color=CLASSIC_BLUE)
    pages.append(p4)

    # ------------------------------------------------------------------
    # PAGE 5: HYPERLOCAL STORE LOCATIONS & CLEAN INVENTORY
    # ------------------------------------------------------------------
    p5, d5 = new_page(
        "Hyperlocal Store Locations & Clean Store Inventory",
        "GeoHash-6 Spatial Indexing & Standardized INR Prices per 100g Across 4 Indian Regions",
        5,
        total_pages,
    )
    d5.text((48, 148), "Sample Curated Store Offers (Zone 3: local_store_inventory)", fill=CLASSIC_BLUE, font=f_sec)
    inv_sample = inv_df[inv_df["in_stock"] == True].head(18)[  # noqa: E712
        ["offer_id", "store_id", "store_name", "region", "city", "geohash_6", "canonical_name", "price_per_100g_inr", "stock_qty"]
    ]
    draw_table(d5, inv_sample, 48, 188, [160, 130, 310, 155, 145, 130, 290, 185, 120], row_h=43, header_color=CLASSIC_BLUE)
    pages.append(p5)

    # ------------------------------------------------------------------
    # PAGE 6: GOLD EXPLAINABLE RECOMMENDATIONS & LP MEAL PLANS
    # ------------------------------------------------------------------
    p6, d6 = new_page(
        "Gold Output: Explainable Protein Recommendations & LP Cost Savings",
        "Zone 5 (/analytics/gold_user_recommendations) — Choice, Cost, Distance & DIAAS Integrated",
        6,
        total_pages,
    )
    d6.text((48, 148), "Sample Top-Ranked Hyperlocal Recommendations (PII Anonymized via SHA-256)", fill=EMERALD, font=f_sec)
    rec_sample = recs_df.head(14)[
        ["user_hash_id", "city", "diet_type", "daily_budget_inr", "rank", "canonical_name", "distance_km", "explanation_text"]
    ]
    y_g = draw_table(d6, rec_sample, 48, 188, [180, 135, 125, 145, 75, 250, 115, 630], row_h=44, header_color=EMERALD)
    d6.text(
        (48, y_g + 26),
        "Note: Full 100,000+ raw CSV rows reside in smartprotein/data/raw/; this PDF summarizes schemas, DQ metrics, and representative records.",
        fill=ANTHRACITE,
        font=f_note,
    )
    pages.append(p6)

    OUT_PDF.parent.mkdir(parents=True, exist_ok=True)
    pages[0].save(
        str(OUT_PDF),
        "PDF",
        resolution=150.0,
        save_all=True,
        append_images=pages[1:],
    )
    size_kb = round(OUT_PDF.stat().st_size / 1024.0, 1)
    print(f"[PDF Export] Created {len(pages)}-page PDF ({size_kb} KB) -> {OUT_PDF}")


if __name__ == "__main__":
    main()

