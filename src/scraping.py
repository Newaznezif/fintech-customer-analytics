"""
src/scraping.py
Scrape Google Play reviews for CBE, BOA, Dashen. Save per-bank CSVs and combined CSV.
Optional: write scraped rows into PostgreSQL (fintech_reviews.reviews).
"""

import os
import time
import yaml
import pandas as pd
from datetime import datetime
from google_play_scraper import reviews, Sort, app
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from pathlib import Path
from typing import Dict, List

# ----------------------
# Helpers: load config
# ----------------------
ROOT = Path(__file__).resolve().parents[1]

def load_yaml(path: Path) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

SCRAPER_CFG = load_yaml(ROOT / "config" / "scraper_config.yaml")
DB_CFG = load_yaml(ROOT / "config" / "db_config.yaml")

APP_IDS = SCRAPER_CFG["app_ids"]
BANK_NAMES = SCRAPER_CFG["bank_names"]
REVIEWS_PER_BANK = int(SCRAPER_CFG.get("reviews_per_bank", 400))
LANG = SCRAPER_CFG.get("lang", "en")
COUNTRY = SCRAPER_CFG.get("country", "us")
MAX_RETRIES = int(SCRAPER_CFG.get("max_retries", 3))
DATA_PATHS = SCRAPER_CFG["data_paths"]

# Ensure raw folder exists
os.makedirs(DATA_PATHS["raw_folder"], exist_ok=True)

# ----------------------
# Scrape functions
# ----------------------
def fetch_app_info(app_id: str) -> Dict:
    """Fetch basic app metadata."""
    try:
        info = app(app_id, lang=LANG, country=COUNTRY)
        return {
            "app_id": app_id,
            "title": info.get("title"),
            "score": info.get("score"),
            "ratings": info.get("ratings"),
            "reviews": info.get("reviews"),
            "installs": info.get("installs")
        }
    except Exception as e:
        print(f"[app_info] error for {app_id}: {e}")
        return {}

def scrape_reviews_for_app(app_id: str, count: int = 400) -> List[Dict]:
    """
    Use google_play_scraper.reviews to fetch 'count' reviews.
    Returns list of raw review dicts.
    """
    for attempt in range(MAX_RETRIES):
        try:
            result, _ = reviews(
                app_id,
                lang=LANG,
                country=COUNTRY,
                sort=Sort.NEWEST,
                count=count,
                filter_score_with=None
            )
            return result
        except Exception as e:
            print(f"[scrape] attempt {attempt+1} failed for {app_id}: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(3)
            else:
                print(f"[scrape] giving up on {app_id} after {MAX_RETRIES} attempts")
                return []
    return []

def process_reviews(raw_reviews: List[Dict], bank_code: str) -> List[Dict]:
    """Normalize raw review dicts into consistent schema rows."""
    rows = []
    bank_name = BANK_NAMES.get(bank_code, bank_code)
    for r in raw_reviews:
        rows.append({
            "review_id": r.get("reviewId") or "",
            "review_text": r.get("content") or "",
            "rating": int(r.get("score") or 0),
            "review_date": pd.to_datetime(r.get("at")) if r.get("at") else pd.NaT,
            "user_name": r.get("userName") or "",
            "thumbs_up": int(r.get("thumbsUpCount") or 0),
            "reply_content": r.get("replyContent") or None,
            "bank_code": bank_code,
            "bank_name": bank_name,
            "app_id": r.get("reviewCreatedVersion") or "",
            "source": "Google Play"
        })
    return rows

# ----------------------
# Save CSV utilities
# ----------------------
def save_df(df: pd.DataFrame, path: str):
    os.makedirs(Path(path).parent, exist_ok=True)
    df.to_csv(path, index=False)
    print(f"[save] wrote {len(df)} rows to {path}")

# ----------------------
# DB insertion (optional)
# ----------------------
def get_db_engine(db_cfg: Dict):
    user = db_cfg["username"]
    pwd = db_cfg["password"]
    host = db_cfg["host"]
    port = db_cfg["port"]
    db = db_cfg["database"]
    driver = db_cfg.get("driver", "postgresql")
    url = f"{driver}://{user}:{pwd}@{host}:{port}/{db}"
    engine = create_engine(url, future=True)
    return engine

def insert_reviews_to_db(df: pd.DataFrame, engine):
    """Insert rows into reviews table. Uses simple INSERT; avoid duplicates by review_id if needed."""
    with engine.begin() as conn:
        for _, row in df.iterrows():
            try:
                stmt = text("""
                INSERT INTO reviews (review_id, review_text, rating, review_date, user_name,
                                     thumbs_up, reply_content, bank_code, bank_name, app_id, source)
                VALUES (:review_id, :review_text, :rating, :review_date, :user_name,
                        :thumbs_up, :reply_content, :bank_code, :bank_name, :app_id, :source)
                ON CONFLICT (review_id) DO NOTHING;
                """)
                conn.execute(stmt, {
                    "review_id": row["review_id"],
                    "review_text": row["review_text"],
                    "rating": int(row["rating"]) if not pd.isna(row["rating"]) else None,
                    "review_date": row["review_date"].to_pydatetime() if not pd.isna(row["review_date"]) else None,
                    "user_name": row["user_name"],
                    "thumbs_up": int(row["thumbs_up"]) if not pd.isna(row["thumbs_up"]) else 0,
                    "reply_content": row["reply_content"],
                    "bank_code": row["bank_code"],
                    "bank_name": row["bank_name"],
                    "app_id": row["app_id"],
                    "source": row["source"]
                })
            except SQLAlchemyError as e:
                print(f"[db insert] error inserting row: {e}")

# ----------------------
# Main orchestration
# ----------------------
def scrape_all(save_to_db: bool = False):
    all_rows = []
    app_info_rows = []

    # 1) fetch app info for each bank
    for code, app_id in APP_IDS.items():
        print(f"\n--- App info for {code} ---")
        info = fetch_app_info(app_id)
        if info:
            info["bank_code"] = code
            info["bank_name"] = BANK_NAMES.get(code)
            app_info_rows.append(info)

    if app_info_rows:
        app_info_df = pd.DataFrame(app_info_rows)
        save_df(app_info_df, DATA_PATHS["app_info"])

    # 2) scrape each bank
    for code, app_id in APP_IDS.items():
        print(f"\nScraping {code} -> {BANK_NAMES.get(code)}")
        raw = scrape_reviews_for_app(app_id, REVIEWS_PER_BANK)
        processed = process_reviews(raw, code)
        if processed:
            df_bank = pd.DataFrame(processed)
            # write per bank
            path = DATA_PATHS.get(f"{code.lower()}_raw")
            if path:
                save_df(df_bank, path)
            all_rows.extend(processed)
        else:
            print(f"[warn] no reviews for {code}")

        time.sleep(2)  # polite pause

    # 3) write combined csv
    if all_rows:
        all_df = pd.DataFrame(all_rows)
        # normalize date column to ISO
        if "review_date" in all_df.columns:
            all_df["review_date"] = pd.to_datetime(all_df["review_date"], errors="coerce").dt.tz_localize(None)
        save_df(all_df, DATA_PATHS["all_raw"])
        print(f"\nScrape complete: total rows = {len(all_df)}")
    else:
        print("[error] no reviews collected at all")
        return None

    # 4) optional db insert
    if save_to_db:
        try:
            engine = get_db_engine(DB_CFG)
            insert_reviews_to_db(all_df, engine)
            print("[db] inserted reviews into DB")
        except Exception as e:
            print(f"[db] error connecting or inserting: {e}")

    return all_df

# ----------------------
# CLI
# ----------------------
if __name__ == "__main__":
    # default: save CSVs only. To insert into DB set save_to_db=True (or run with env var)
    df = scrape_all(save_to_db=False)
