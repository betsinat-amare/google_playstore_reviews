import pandas as pd
import spacy
from sklearn.feature_extraction.text import TfidfVectorizer

INPUT = "data/processed/sentiment_reviews.csv"
OUTPUT = "data/processed/keywords_reviews.csv"

def extract_keywords_tfidf(corpus, top_n=10):
    """Extract the top TF-IDF keywords from a list of texts."""
    vectorizer = TfidfVectorizer(
        stop_words="english",
        ngram_range=(1, 2),     # single words + bigrams
        max_features=2000
    )

    tfidf_matrix = vectorizer.fit_transform(corpus)
    feature_names = vectorizer.get_feature_names_out()

    # average TF-IDF scores across all documents
    avg_scores = tfidf_matrix.mean(axis=0).A1
    sorted_indices = avg_scores.argsort()[::-1]

    top_keywords = [feature_names[i] for i in sorted_indices[:top_n]]
    return top_keywords


def extract_noun_phrases_spacy(nlp, text):
    """Extract noun phrases using spaCy."""
    doc = nlp(text)
    return [chunk.text.lower() for chunk in doc.noun_chunks]


def main():
    print("Loading dataset...")
    df = pd.read_csv(INPUT)

    print("Loading spaCy model...")
    nlp = spacy.load("en_core_web_sm")

    print("Extracting TF-IDF keywords per bank...")
    bank_keywords = {}

    for bank in df["bank"].unique():
        bank_df = df[df["bank"] == bank]
        keywords = extract_keywords_tfidf(bank_df["review"].astype(str).tolist())
        bank_keywords[bank] = keywords
        print(f"\nTop keywords for {bank}:")
        print(keywords)

    print("\nExtracting noun phrases...")
    df["noun_phrases"] = df["review"].astype(str).apply(lambda x: extract_noun_phrases_spacy(nlp, x))

    print(f"\nSaving output to {OUTPUT} ...")
    df.to_csv(OUTPUT, index=False)

    print("\n✔ Keyword extraction complete.")


if __name__ == "__main__":
    main()
