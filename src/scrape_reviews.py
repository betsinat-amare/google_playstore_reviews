# src/scrape_reviews.py

import time
import pandas as pd
from tqdm import tqdm
from google_play_scraper import reviews, Sort

# ---- CONFIG ----
TARGET_REVIEWS = 500        # aim for 500 per bank (minimum 400)
SLEEP_SECONDS = 0.3         # polite delay between requests
RAW_OUTPUT = "data/raw/raw_reviews.csv"

APPS = [
    {
        "bank": "Commercial Bank of Ethiopia",
        "package": "com.combanketh.mobilebanking"
    },
    {
        "bank": "Bank of Abyssinia",
        "package": "com.boa.boaMobileBanking"
    },
    {
        "bank": "Dashen Bank",
        "package": "com.dashen.dashensuperapp"
    }
]

def fetch_app_reviews(app_package, bank_name, target=400):
    all_reviews = []
    token = None

    pbar = tqdm(total=target, desc=f"Scraping {bank_name}")

    while len(all_reviews) < target:
        result, token = reviews(
            app_package,
            lang='en',
            country='us',
            sort=Sort.NEWEST,
            count=200,
            continuation_token=token
        )

        if not result:
            break

        for r in result:
            all_reviews.append({
                "review": r.get("content"),
                "rating": r.get("score"),
                "date": r.get("at").isoformat() if r.get("at") else None,
                "bank": bank_name,
                "source": "Google Play"
            })

            if len(all_reviews) >= target:
                break

        pbar.update(len(result))
        time.sleep(SLEEP_SECONDS)

        if token is None:
            break

    pbar.close()
    return all_reviews


def main():
    collected = []

    for app in APPS:
        bank = app["bank"]
        pkg = app["package"]

        print(f"Starting scrape for {bank}...")
        reviews_list = fetch_app_reviews(pkg, bank, TARGET_REVIEWS)
        print(f"Collected {len(reviews_list)} reviews for {bank}.\n")

        collected.extend(reviews_list)

    df = pd.DataFrame(collected)
    df.to_csv(RAW_OUTPUT, index=False)
    print(f"Saved raw review data to {RAW_OUTPUT}")
    print(f"Total reviews collected: {len(df)}")


if __name__ == "__main__":
    main()
