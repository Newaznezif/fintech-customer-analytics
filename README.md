README.md Update (Task 1 Methodology)
 FinTech Customer Analytics - Task 1

 Task 1: Data Collection and Preprocessing

 Objective
Collect customer reviews from Google Play for three banks (CBE, BOA, Dashen), clean and preprocess the data for further analysis.

 Steps

1. Web Scraping
   - Used `google-play-scraper` Python library to scrape app reviews.
   - Targeted at least 400 reviews per bank, collecting a total of ~1,800 reviews.
   - Scraped data includes: review text, rating, review date, reviewer name, thumbs up, app version, and bank info.
   - Saved raw data to `data/raw/`.

2. Preprocessing
   - Created a preprocessing script (`src/preprocessing.py`) to clean and normalize data.
   - Steps include:
     - Removing duplicate reviews (based on `review_id` and `review_text`).
     - Handling missing values (`review_text`, `rating`, `bank_name`, `source`).
     - Normalizing dates to `YYYY-MM-DD`.
     - Saving cleaned CSVs to `data/cleaned/`.

3. Deliverables
   - Raw CSVs: `data/raw/all_reviews.csv`, `cbe_raw.csv`, `boa_raw.csv`, `dashen_raw.csv`.
   - Cleaned CSVs: `data/cleaned/all_reviews.csv`, `cbe_cleaned.csv`, `boa_cleaned.csv`, `dashen_cleaned.csv`.
   - Preprocessing script: `src/preprocessing.py`.

 KPIs
- 1,800+ reviews collected with <5% missing data.
- Clean, normalized CSV dataset ready for analysis.
- Task code version-controlled on GitHub (`task-1` branch) with meaningful commits.

## Task 3: PostgreSQL Database Schema

### Tables

**app_info**
| Column   | Type    | Key         | Description                   |
|----------|---------|------------|-------------------------------|
| app_id   | INT     | PRIMARY KEY | Unique ID for the bank app     |
| bank_name| TEXT    |             | Name of the bank              |

**reviews**
| Column           | Type       | Key         | Description                         |
|-----------------|------------|------------|-------------------------------------|
| review_id       | UUID       | PRIMARY KEY | Unique review identifier            |
| app_id          | INT        | FOREIGN KEY | Links to app_info.app_id            |
| review_text     | TEXT       |             | User review content                 |
| rating          | INT        |             | Star rating (1–5)                  |
| review_date     | DATE       |             | Date of review                      |
| sentiment_label | TEXT       |             | positive, neutral, or negative      |
| sentiment_score | FLOAT      |             | Sentiment confidence score          |
| source          | TEXT       |             | Source of the review (e.g., Google Play) |
