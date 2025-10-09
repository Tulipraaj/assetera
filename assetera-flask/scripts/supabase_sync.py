import os
import snowflake.connector
import psycopg2
from datetime import date, timedelta

# ------------------------------------------------------------
# 1. Connections
# ------------------------------------------------------------
sf = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DB"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
)

pg = psycopg2.connect(os.getenv("SUPABASE_DATABASE_URL"))
pg.autocommit = True
pg_cursor = pg.cursor()

# ------------------------------------------------------------
# 2. Ensure control table exists in Supabase
# ------------------------------------------------------------
pg_cursor.execute("""
CREATE TABLE IF NOT EXISTS sync_state (
    id SERIAL PRIMARY KEY,
    last_synced_date DATE
);
""")

# If empty, insert initial sync state (e.g., 10 years ago)
pg_cursor.execute("SELECT last_synced_date FROM sync_state ORDER BY id DESC LIMIT 1;")
row = pg_cursor.fetchone()
if row is None or row[0] is None:
    start_date = date.today() - timedelta(days=365 * 10)
    pg_cursor.execute("INSERT INTO sync_state (last_synced_date) VALUES (%s);", (start_date,))
    last_synced_date = start_date
else:
    last_synced_date = row[0]

print(f"Last synced date: {last_synced_date}")

# ------------------------------------------------------------
# 3. Fetch only new data from Snowflake
# ------------------------------------------------------------
query = f"""
SELECT ticker, date, price
FROM daily_prices
WHERE date > '{last_synced_date}'
ORDER BY date;
"""

sf_cur = sf.cursor()
sf_cur.execute(query)
rows = sf_cur.fetchall()

if not rows:
    print("No new data to sync.")
else:
    print(f"Fetched {len(rows)} new rows from Snowflake")

    # ------------------------------------------------------------
    # 4. Insert or upsert into Supabase (Postgres)
    # ------------------------------------------------------------
    pg_cursor.execute("""
    CREATE TABLE IF NOT EXISTS daily_prices (
        ticker TEXT NOT NULL,
        date DATE NOT NULL,
        price DOUBLE PRECISION,
        PRIMARY KEY (ticker, date)
    );
    """)
    insert_query = """
    INSERT INTO daily_prices (ticker, date, adj_close)
    VALUES (%s, %s, %s)
    ON CONFLICT (ticker, date) DO UPDATE
    SET price = EXCLUDED.adj_close;
    """
    pg_cursor.executemany(insert_query, rows)

    # ------------------------------------------------------------
    # 5. Delete old (>10 years) data from Supabase
    # ------------------------------------------------------------
    cutoff = date.today() - timedelta(days=365 * 10)
    pg_cursor.execute("DELETE FROM daily_prices WHERE date < %s;", (cutoff,))

    # ------------------------------------------------------------
    # 6. Update last synced date
    # ------------------------------------------------------------
    latest_date = max(r[1] for r in rows)
    pg_cursor.execute("UPDATE sync_state SET last_synced_date = %s;", (latest_date,))
    print(f"Updated sync state to {latest_date}")

sf_cur.close()
pg_cursor.close()
pg.close()
sf.close()

print("✅ Incremental sync completed successfully.")
