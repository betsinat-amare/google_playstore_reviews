import psycopg2

# Your cleaned reviews list
cleaned_reviews = [
    {
        "bank_name": "Commercial Bank of Ethiopia",
        "app_name": "CBE Mobile",
        "review_text": "Love the app but it crashes sometimes",
        "rating": 4,
        "review_date": "2025-12-01",
        "sentiment_label": "positive",
        "sentiment_score": 0.8
    }
    # ... add all your other reviews here
]

# Connect to PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    database="bank_reviews",
    user="postgres",
    password="backspace"  # replace with your password
)

cur = conn.cursor()

# Insert banks and reviews
bank_cache = {}

for review in cleaned_reviews:
    bank_key = (review["bank_name"], review["app_name"])
    
    if bank_key not in bank_cache:
        cur.execute("""
            INSERT INTO banks (bank_name, app_name)
            VALUES (%s, %s)
            ON CONFLICT (bank_name) DO NOTHING
            RETURNING bank_id
        """, (review["bank_name"], review["app_name"]))
        
        result = cur.fetchone()
        if result:
            bank_cache[bank_key] = result[0]
        else:
            cur.execute("SELECT bank_id FROM banks WHERE bank_name=%s", (review["bank_name"],))
            bank_cache[bank_key] = cur.fetchone()[0]

    bank_id = bank_cache[bank_key]

    cur.execute("""
        INSERT INTO reviews (bank_id, review_text, rating, review_date, sentiment_label, sentiment_score)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        bank_id,
        review["review_text"],
        review["rating"],
        review["review_date"],
        review["sentiment_label"],
        review["sentiment_score"]
    ))

conn.commit()
cur.close()
conn.close()
print("Data inserted successfully!")
