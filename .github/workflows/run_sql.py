import os
import glob
import snowflake.connector

# Connect using secrets
conn = snowflake.connector.connect(
    account=os.getenv("SNOW_ACCOUNT"),
    user=os.getenv("SNOW_USER"),
    password=os.getenv("SNOW_PASSWORD"),
    role=os.getenv("SNOW_ROLE"),
    warehouse=os.getenv("SNOW_WAREHOUSE"),
    database=os.getenv("SNOW_DATABASE"),
    schema=os.getenv("SNOW_SCHEMA"),
)

cur = conn.cursor()

# 1. Ensure stage exists
print("▶ Ensuring repo_stage exists...")
cur.execute("""
    CREATE STAGE IF NOT EXISTS repo_stage
    FILE_FORMAT = (TYPE=CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"')
""")

# 2. Upload CSV files into stage
csv_files = glob.glob("data/csv/*.csv")
if csv_files:
    for file in csv_files:
        filename = os.path.basename(file)
        print(f"▶ Uploading {file} → {filename}.gz")
        cur.execute(f"PUT file://{file} @repo_stage AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
else:
    print("⚠️ No CSV files found in data/csv/")

# 3. List files in stage (for debugging)
print("▶ Files currently in repo_stage:")
cur.execute("LIST @repo_stage")
for row in cur.fetchall():
    print("   ", row)

# 4. Run SQL scripts in proper order
folders = ["migrations", "tables", "constraints", "data", "views", "scripts"]
for folder in folders:
    if folder == "migrations/admin_only":
        continue
    if os.path.isdir(folder):
        for file in sorted(os.listdir(folder)):
            if file.endswith(".sql"):
                path = os.path.join(folder, file)
                print(f"▶ Running {path}")
                with open(path) as f:
                    sql = f.read()
                    for stmt in sql.split(";"):
                        if stmt.strip():
                            try:
                                cur.execute(stmt)
                            except Exception as e:
                                print(f"❌ Error in {file}: {e}")
                                raise

cur.close()
conn.close()
print("✅ Deployment completed successfully")
