import pandas as pd
from pathlib import Path
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from sklearn.feature_extraction.text import TfidfVectorizer
import re

# -------------------------
# Paths
# -------------------------
RAW_FOLDER = Path("data/cleaned")
OUTPUT_FILE = Path("data/outputs/review_sentiment_themes.csv")
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

RAW_FILES = [
    RAW_FOLDER / "all_reviews.csv",
    RAW_FOLDER / "cbe_cleaned.csv",
    RAW_FOLDER / "boa_cleaned.csv",
    RAW_FOLDER / "dashen_cleaned.csv"
]

# -------------------------
# Sentiment Analyzer
# -------------------------
analyzer = SentimentIntensityAnalyzer()

def get_sentiment(text):
    score = analyzer.polarity_scores(text)["compound"]
    if score >= 0.05:
        label = "positive"
    elif score <= -0.05:
        label = "negative"
    else:
        label = "neutral"
    return score, label

# -------------------------
# Theme Extraction
# -------------------------
# Define bank-specific keywords mapping to themes
THEME_KEYWORDS = {
    "Account Access Issues": ["login", "password", "access", "account blocked"],
    "Transaction Performance": ["transfer", "slow", "pending", "failed", "delay"],
    "User Interface & Experience": ["UI", "design", "app layout", "navigation"],
    "Customer Support": ["support", "help", "call", "chat", "response"],
    "Feature Requests": ["feature", "add", "improve", "wish"]
}

def extract_theme(text):
    text_lower = text.lower()
    themes = []
    for theme, keywords in THEME_KEYWORDS.items():
        for kw in keywords:
            if re.search(r"\b" + re.escape(kw) + r"\b", text_lower):
                themes.append(theme)
                break
    if not themes:
        themes.append("Other")
    return "; ".join(themes)

# -------------------------
# Process all cleaned CSVs
# -------------------------
all_reviews = []

for file in RAW_FILES:
    if file.exists():
        df = pd.read_csv(file)
        df["sentiment_score"], df["sentiment_label"] = zip(*df["review_text"].map(get_sentiment))
        df["identified_theme"] = df["review_text"].map(extract_theme)
        all_reviews.append(df)
    else:
        print(f"[Warning] {file} does not exist, skipping.")

# -------------------------
# Save combined output
# -------------------------
if all_reviews:
    combined_df = pd.concat(all_reviews, ignore_index=True)
    combined_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Sentiment and theme analysis complete! Output saved to {OUTPUT_FILE}")
else:
    print("[Error] No files processed. Check your cleaned CSVs.")
