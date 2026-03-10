# db/schema_apply.py
# Applies schema.sql to the database (idempotent — uses CREATE TABLE IF NOT EXISTS)
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
with open(schema_path) as f:
    sql = f.read()

conn = psycopg2.connect(os.environ["DATABASE_URL"])
try:
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    print("[schema_apply] Schema applied successfully.")
finally:
    conn.close()
