import sqlite3

c = sqlite3.connect("data/pipeline.db")

for table, label in [("job_signals", "JOBS"), ("news_signals", "NEWS")]:
    print(f"\n=== {label}: 5 random links ===")
    rows = c.execute(
        f"SELECT title, source_url, published FROM {table} ORDER BY RANDOM() LIMIT 5"
    ).fetchall()
    for title, url, published in rows:
        print(f"\nTitle:     {title}")
        print(f"Published: {published}")
        print(f"Link:      {url}")