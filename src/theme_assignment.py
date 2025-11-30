import pandas as pd

INPUT = "data/processed/keywords_reviews.csv"
OUTPUT = "data/processed/themes_reviews.csv"

# Define themes → keywords mapping
THEME_KEYWORDS = {
    "Login / Access Issues": ["login", "password", "pin", "cannot login", "authenticate", "login fail", "login error"],
    "App Performance / Crashes": ["slow", "crash", "freezes", "stopped", "timeout", "lag", "force close"],
    "Transactions / Transfers": ["transfer", "payment", "transaction failed", "network error", "processing", "send money"],
    "UI / UX": ["user friendly", "ui", "easy to use", "navigation", "good design", "experience"],
    "Feature Requests / Suggestions": ["fingerprint", "biometric", "offline", "feature", "qr", "notification"]
}

def assign_themes(review_text):
    review_text = str(review_text).lower()
    matched_themes = []
    for theme, keywords in THEME_KEYWORDS.items():
        for kw in keywords:
            if kw in review_text:
                matched_themes.append(theme)
                break
    return "|".join(sorted(set(matched_themes)))

def main():
    print("Loading dataset...")
    df = pd.read_csv(INPUT)

    print("Assigning themes to reviews...")
    df["themes"] = df["review"].apply(assign_themes)

    print(f"Saving themes to {OUTPUT} ...")
    df.to_csv(OUTPUT, index=False)

    print("✔ Theme assignment complete.")
    print("\nSample themes:")
    print(df[["review","themes"]].head(10))

if __name__ == "__main__":
    main()
