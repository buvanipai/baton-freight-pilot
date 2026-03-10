# db/seed_restore.py
# Restores seed_dump.sql into the database if it hasn't been seeded yet.
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(os.environ["DATABASE_URL"])
try:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM carriers")
        if cur.fetchone()[0] > 0:
            print("[seed_restore] Data already present, skipping.")
            conn.close()
            exit(0)

    dump_path = os.path.join(os.path.dirname(__file__), "seed_dump.sql")
    with open(dump_path) as f:
        sql = f.read()

    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    print("[seed_restore] Seed dump restored successfully.")
finally:
    conn.close()
