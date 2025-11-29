# src/preprocess_reviews.py

import pandas as pd
from dateutil import parser as dateparser

RAW_INPUT = "data/raw/raw_reviews.csv"
CLEAN_OUTPUT = "data/clean/clean_reviews.csv"


def normalize_date(d):
    if pd.isna(d) or d is None:
        return None
    try:
        dt = dateparser.parse(d)
        return dt.strftime("%Y-%m-%d")
    except:
        return None


def main():
    print("Loading raw dataset...")
    df = pd.read_csv(RAW_INPUT)

    print(f"Initial rows: {len(df)}")

    # Drop missing essential columns
    df = df.dropna(subset=["review", "rating"])
    print(f"After dropping missing review/rating: {len(df)}")

    # Normalize date formatting
    df["date"] = df["date"].apply(normalize_date)

    # Remove duplicates (same text + same rating + same bank)
    before = len(df)
    df = df.drop_duplicates(subset=["review", "rating", "bank"])
    print(f"Removed duplicates: {before - len(df)}")

    # Ensure column order
    df = df[["review", "rating", "date", "bank", "source"]]

    # Save clean dataset
    df.to_csv(CLEAN_OUTPUT, index=False)

    print("\n✔ Saved cleaned dataset to:", CLEAN_OUTPUT)
    print("Final row count:", len(df))

    # KPI check
    missing_pct = df.isnull().mean() * 100
    print("\n--- Missing Data % by Column ---")
    print(missing_pct)
    print("--------------------------------")

    print("\nPreprocessing completed successfully.")


if __name__ == "__main__":
    main()
