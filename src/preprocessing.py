import pandas as pd
from pathlib import Path

# Paths
RAW_FOLDER = Path("data/raw")
PROCESSED_FOLDER = Path("data/cleaned")
PROCESSED_FOLDER.mkdir(parents=True, exist_ok=True)

RAW_FILES = [
    RAW_FOLDER / "all_reviews.csv",
    RAW_FOLDER / "cbe_raw.csv",
    RAW_FOLDER / "boa_raw.csv",
    RAW_FOLDER / "dashen_raw.csv"
]

def preprocess_csv(input_path: Path, output_path: Path):
    # Load CSV
    df = pd.read_csv(input_path)

    # Remove duplicates based on review_id or review_text
    df = df.drop_duplicates(subset=["review_id", "review_text"])

    # Handle missing data
    df["review_text"] = df["review_text"].fillna("")
    df["rating"] = df["rating"].fillna(0).astype(int)
    df["review_date"] = pd.to_datetime(df["review_date"], errors="coerce")

    # Normalize date format to YYYY-MM-DD
    df["review_date"] = df["review_date"].dt.strftime("%Y-%m-%d")

    # Ensure bank_name and source exist
    df["bank_name"] = df["bank_name"].fillna("Unknown")
    df["source"] = df["source"].fillna("Google Play")

    # Save processed CSV
    df.to_csv(output_path, index=False)
    print(f"[Preprocessing] Cleaned {input_path.name} -> {output_path.name} ({len(df)} rows)")

# Process all files
for raw_file in RAW_FILES:
    output_file = PROCESSED_FOLDER / raw_file.name.replace("_raw", "_cleaned")
    preprocess_csv(raw_file, output_file)

print("✅ Preprocessing complete!")
