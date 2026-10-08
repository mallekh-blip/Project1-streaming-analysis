import os, psycopg2
from dotenv import load_dotenv
load_dotenv()
conn = psycopg2.connect(host=os.environ["DATABASE_HOST"], dbname=os.environ["DATABASE_NAME"],
                        user=os.environ["DATABASE_USER"], password=os.environ["DATABASE_PASSWORD"], sslmode="require")
cur = conn.cursor()
cur.execute("SELECT COUNT(*), SUM(is_injected::int), MAX(ts) FROM stream.fact_event_stream")
print("Rows: %s | Injected anomalies: %s | Latest: %s" % cur.fetchone())
conn.close()