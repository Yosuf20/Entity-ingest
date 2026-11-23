import sqlite3, json

c = sqlite3.connect("data/pipeline.db")
c.row_factory = sqlite3.Row

tables = [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")]
print("TABLES:", tables)

for t in tables:
    if "paper" in t.lower():
        n = c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"\n=== {t}: {n} rows ===")
        for r in c.execute(f"SELECT * FROM {t} ORDER BY RANDOM() LIMIT 5"):
            d = dict(r)
            for k, v in d.items():
                if isinstance(v, str) and len(v) > 150:
                    d[k] = v[:150] + "..."
            print(json.dumps(d, indent=2, default=str))