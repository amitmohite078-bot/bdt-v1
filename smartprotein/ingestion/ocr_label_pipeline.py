"""SmartProtein Stage 5 (OPTIONAL Module #10): Nutrition-Label OCR Pipeline.

1. Generates synthetic nutrition-label PNG images in data/raw/label_images/ (for testing).
2. Runs Tesseract OCR (via pytesseract if binary is installed; otherwise uses a clearly
   logged text-sidecar fallback) to extract Protein, Carbs, Fat, and Energy per 100 g.
3. Emits standardized CSV rows tagged source_db='OCR_LABEL' into raw/food_composition/
   so OCR-parsed products pass through the exact same Spark DQ & Atwater pipeline.
"""

from __future__ import annotations

import argparse
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from PIL import Image, ImageDraw, ImageFont

ROOT_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT_DIR / "data" / "raw"
LABEL_IMG_DIR = RAW_DIR / "label_images"

SAMPLE_LABELS = [
    {
        "filename": "label_paneer_valid.png",
        "food_name": "High-Protein Malai Paneer (OCR)",
        "category": "Dairy",
        "diet_type": "Veg",
        "allergens": "dairy|lactose",
        "text": (
            "NUTRITION FACTS (Per 100g)\n"
            "Energy: 224 kcal\n"
            "Protein: 21.0 g\n"
            "Carbohydrate: 3.5 g\n"
            "Total Fat: 14.0 g\n"
            "Dietary Fiber: 0.0 g\n"
        ),
    },
    {
        "filename": "label_soy_corrupt_ocr.png",
        "food_name": "Misprinted Soy Crisp (OCR)",
        "category": "Legumes_Soy",
        "diet_type": "Vegan",
        "allergens": "soy",
        "text": (
            "NUTRITION FACTS (Per 100g)\n"
            "Energy: 120 kcal\n"
            "Protein: 135.0 g\n"
            "Carbohydrate: 20.0 g\n"
            "Total Fat: 2.0 g\n"
            "Dietary Fiber: 5.0 g\n"
        ),
    },
]


def create_sample_label_images(img_dir: Path) -> None:
    img_dir.mkdir(parents=True, exist_ok=True)
    font = ImageFont.load_default()
    for item in SAMPLE_LABELS:
        img = Image.new("RGB", (420, 220), color=(255, 255, 255))
        draw = ImageDraw.Draw(img)
        draw.rectangle([(8, 8), (412, 212)], outline=(0, 0, 0), width=2)
        draw.multiline_text((22, 24), item["text"], fill=(0, 0, 0), font=font, spacing=8)
        img.save(img_dir / item["filename"])
        # Save sidecar metadata + text fallback so pipeline stays testable without Tesseract binary
        (img_dir / f"{item['filename']}.meta.txt").write_text(
            f"{item['food_name']}|{item['category']}|{item['diet_type']}|{item['allergens']}\n{item['text']}",
            encoding="utf-8",
        )


def parse_nutrition_text(ocr_text: str) -> dict[str, float | None]:
    def _extract(pattern: str) -> float | None:
        m = re.search(pattern, ocr_text, flags=re.IGNORECASE)
        return float(m.group(1)) if m else None

    return {
        "reported_kcal": _extract(r"Energy\s*[:\-]?\s*(\d+(?:\.\d+)?)\s*kcal"),
        "protein_g": _extract(r"Protein\s*[:\-]?\s*(\d+(?:\.\d+)?)\s*g"),
        "carbs_g": _extract(r"Carbohydrate[s]?\s*[:\-]?\s*(\d+(?:\.\d+)?)\s*g"),
        "fat_g": _extract(r"(?:Total\s*)?Fat\s*[:\-]?\s*(\d+(?:\.\d+)?)\s*g"),
        "fiber_g": _extract(r"(?:Dietary\s*)?Fiber\s*[:\-]?\s*(\d+(?:\.\d+)?)\s*g") or 0.0,
    }


def run_ocr(img_path: Path) -> tuple[str, str]:
    tesseract_bin = shutil.which("tesseract")
    if tesseract_bin:
        try:
            import pytesseract

            text = pytesseract.image_to_string(Image.open(img_path))
            return text, "TESSERACT_OCR"
        except Exception:
            pass
    # Fallback when OS Tesseract binary / pytesseract is not installed
    sidecar = img_path.with_name(f"{img_path.name}.meta.txt")
    if sidecar.exists():
        lines = sidecar.read_text(encoding="utf-8").splitlines()[1:]
        return "\n".join(lines), "FALLBACK_TEXT_SIDECAR"
    return "", "NO_OCR_ENGINE"


def main() -> None:
    parser = argparse.ArgumentParser(description="SmartProtein Nutrition-Label OCR Pipeline")
    parser.add_argument("--create-samples", action="store_true", default=True)
    args = parser.parse_args()

    if args.create_samples:
        create_sample_label_images(LABEL_IMG_DIR)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = RAW_DIR / "food_composition" / f"ingestion_date={today}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "ocr_labels_raw.csv"

    rows = []
    for idx, img_path in enumerate(sorted(LABEL_IMG_DIR.glob("*.png")), start=1):
        meta_path = img_path.with_name(f"{img_path.name}.meta.txt")
        meta = meta_path.read_text(encoding="utf-8").splitlines()[0].split("|") if meta_path.exists() else [img_path.stem, "Dairy", "Veg", "none"]
        ocr_text, engine_used = run_ocr(img_path)
        parsed = parse_nutrition_text(ocr_text)
        rows.append(
            {
                "record_id": f"OCR-{idx:05d}",
                "source_db": "OCR_LABEL",
                "food_name": meta[0],
                "category": meta[1],
                "diet_type": meta[2],
                "state_raw_cooked": "raw",
                "serving_unit": "per_100g",
                "protein_g": parsed["protein_g"],
                "carbs_g": parsed["carbs_g"],
                "fat_g": parsed["fat_g"],
                "fiber_g": parsed["fiber_g"],
                "moisture_g": 55.0,
                "reported_kcal": parsed["reported_kcal"],
                "energy_unit": "kcal",
                "iron_mg": 0.2,
                "calcium_mg": 350.0,
                "vitamin_b12_mcg": 0.5,
                "leucine_g_per_100g_protein": 9.2,
                "phytate_mg": 0.0,
                "allergens": meta[3],
                "last_updated": today,
            }
        )
        print(f"[OCR:{engine_used}] {img_path.name} -> Protein={parsed['protein_g']}g, Kcal={parsed['reported_kcal']}")

    pd.DataFrame(rows).to_csv(out_csv, index=False)
    print(f"[OCR Pipeline] Wrote {len(rows)} OCR-parsed records -> {out_csv}")


if __name__ == "__main__":
    main()

