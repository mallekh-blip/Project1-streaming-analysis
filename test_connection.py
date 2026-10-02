import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.environ["DATABASE_HOST"],
    dbname=os.environ["DATABASE_NAME"],
    user=os.environ["DATABASE_USER"],
    password=os.environ["DATABASE_PASSWORD"],
    port=os.environ.get("DATABASE_PORT", "5432"),
    sslmode="require",
)
cur = conn.cursor()
cur.execute("SELECT version();")
print("Connected!", cur.fetchone()[0])
conn.close()