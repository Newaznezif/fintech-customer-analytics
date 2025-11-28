import psycopg2
import pandas as pd
from psycopg2.extras import execute_values

# -----------------------------
# DB CONNECTION
# -----------------------------
conn = psycopg2.connect(
    host="localhost",
    database="fintech_reviews",
    user="postgres",
    password="postgres"
)
cur = conn.cursor()

# -----------------------------
# LOAD CSV
# -----------------------------
df = pd.read_csv("data/outputs/review_sentiment_themes.csv")

# Remove duplicates by review_id
df = df.drop_duplicates(subset=['review_id'])

# -----------------------------
# INSERT INTO app_info
# -----------------------------
# Get unique banks
apps = df[['bank_name']].drop_duplicates().reset_index(drop=True)
# Assign numeric app_id automatically
apps['app_id'] = range(1, len(apps) + 1)

execute_values(
    cur,
    """
    INSERT INTO app_info (app_id, bank_name)
    VALUES %s
    ON CONFLICT (app_id) DO NOTHING;
    """,
    list(apps[['app_id', 'bank_name']].itertuples(index=False, name=None))
)
print("✔ app_info table updated.")

# Map bank_name → app_id in the reviews
bank_to_appid = dict(zip(apps['bank_name'], apps['app_id']))
df['app_id'] = df['bank_name'].map(bank_to_appid)

# -----------------------------
# INSERT INTO reviews
# -----------------------------
review_cols = ['review_id','app_id','review_text','rating','review_date',
               'user_name','thumbs_up','reply_content','bank_code',
               'source','sentiment_score','sentiment_label','identified_theme']

execute_values(
    cur,
    f"""
    INSERT INTO reviews ({', '.join(review_cols)})
    VALUES %s
    ON CONFLICT (review_id) DO NOTHING;
    """,
    list(df[review_cols].itertuples(index=False, name=None))
)
print("✔ reviews table updated.")

# -----------------------------
# COMMIT + CLOSE
# -----------------------------
conn.commit()
cur.close()
conn.close()

print("🎉 All data inserted successfully!")

# -----------------------------
# CHECK SUMMARY
# -----------------------------
print(df['bank_name'].value_counts())
print(df['review_id'].nunique(), len(df))
