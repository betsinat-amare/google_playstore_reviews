# src/task2_sentiment_transformer.py
"""
Compute sentiment using DistilBERT fine-tuned on SST-2.
Fallback to VADER if transformers or torch not installed / out-of-memory.

Outputs:
 - data/processed/sentiment_reviews.csv
Columns:
 - review_id, review, rating, date, bank, source, sentiment_label, sentiment_score, model
"""

import os
import pandas as pd
from tqdm import tqdm

OUT_DIR = "data/processed"
os.makedirs(OUT_DIR, exist_ok=True)
CLEAN_CSV = "data/clean/clean_reviews.csv"
OUT_CSV = os.path.join(OUT_DIR, "sentiment_reviews.csv")

USE_TRANSFORMER = True  # set False to force VADER fallback

def run_transformer(df, model_name="distilbert-base-uncased-finetuned-sst-2-english", batch_size=32, device=-1):
    """
    Run HF pipeline. device=-1 for CPU, or 0 for GPU 0.
    Returns DataFrame with added sentiment columns.
    """
    from transformers import pipeline
    n = len(df)
    pipe = pipeline("sentiment-analysis", model=model_name, device=device)  # device: -1 CPU, >=0 GPU
    labels = []
    scores = []

    for i in tqdm(range(0, n, batch_size), desc="Transformer sentiment"):
        batch_texts = df["review"].iloc[i:i+batch_size].astype(str).tolist()
        results = pipe(batch_texts, truncation=True)
        for res in results:
            labels.append(res["label"].lower())   # 'POSITIVE'/'NEGATIVE'
            scores.append(float(res["score"]))
    df["sentiment_label"] = labels
    df["sentiment_score"] = scores
    df["model"] = model_name
    return df

def run_vader(df):
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    analyzer = SentimentIntensityAnalyzer()
    labels = []
    scores = []
    for t in tqdm(df["review"].astype(str).tolist(), desc="VADER sentiment"):
        v = analyzer.polarity_scores(t)
        comp = v["compound"]
        if comp > 0.05:
            labels.append("positive")
        elif comp < -0.05:
            labels.append("negative")
        else:
            labels.append("neutral")
        scores.append(comp)
    df["sentiment_label"] = labels
    df["sentiment_score"] = scores
    df["model"] = "vader"
    return df

def main():
    df = pd.read_csv(CLEAN_CSV)
    df = df.reset_index(drop=True).reset_index().rename(columns={"index":"review_id"})  # add review_id

    # Keep only required columns for speed
    df = df[["review_id","review","rating","date","bank","source"]]

    # Filter non-empty review text
    df = df[df["review"].notna() & (df["review"].str.strip() != "")]
    df = df.reset_index(drop=True)

    # Attempt transformer
    if USE_TRANSFORMER:
        try:
            # device selection: 0 if CUDA available else -1
            import torch
            device = 0 if torch.cuda.is_available() else -1
            df = run_transformer(df, device=device)
        except Exception as e:
            print("Transformer failed or not available, falling back to VADER. Error:", e)
            df = run_vader(df)
    else:
        df = run_vader(df)

    # Normalize labels: map 'positive'/'negative', if SST labels 'LABEL_1' etc handle that
    df["sentiment_label"] = df["sentiment_label"].str.lower().map(
        {"positive":"positive","negative":"negative","neutral":"neutral",
         "pos":"positive","neg":"negative"}
    ).fillna(df["sentiment_label"].str.lower())

    df.to_csv(OUT_CSV, index=False)
    print("Saved sentiment CSV:", OUT_CSV)
    print(df["sentiment_label"].value_counts())

if __name__ == "__main__":
    main()
