import pandas as pd

INPUT = "data/processed/sentiment_reviews.csv"

def main():
    print("Loading sentiment dataset...")
    df = pd.read_csv(INPUT)

    print("\n=== Sentiment Count by Bank ===")
    by_bank = df.groupby(["bank", "sentiment_label"]).size().unstack(fill_value=0)
    print(by_bank)

    print("\n=== Sentiment Count by Rating ===")
    by_rating = df.groupby(["rating", "sentiment_label"]).size().unstack(fill_value=0)
    print(by_rating)

    print("\n=== Avg Sentiment Score by Rating ===")
    score_avg = df.groupby("rating")["sentiment_score"].mean()
    print(score_avg)

    print("\n✔ Aggregation complete.")

if __name__ == "__main__":
    main()
