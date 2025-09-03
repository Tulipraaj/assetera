import os, glob
import snowflake.connector

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
cur.execute("""
CREATE STAGE IF NOT EXISTS repo_stage
FILE_FORMAT = (TYPE=CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"')
""")

# 2. Upload CSVs into stage
for file in glob.glob("data/csv/*.csv"):
    filename = os.path.basename(file)
    print(f"▶ Uploading {file} → {filename}")
    cur.execute(f"PUT file://{file} @repo_stage AUTO_COMPRESS=TRUE OVERWRITE=TRUE")

# 3. Verify stage contents
print("▶ Files in repo_stage:")
cur.execute("LIST @repo_stage")
for row in cur.fetchall():
    print("   ", row)

# 4. Run SQL scripts
folders = ["migrations", "tables", "constraints", "data", "views", "scripts"]
for folder in folders:
    if os.path.isdir(folder) and not folder.endswith("admin_only"):
        for file in sorted(os.listdir(folder)):
            if file.endswith(".sql"):
                path = os.path.join(folder, file)
                print(f"▶ Running {path}")
                with open(path) as f:
                    for stmt in f.read().split(";"):
                        if stmt.strip():
                            cur.execute(stmt)

cur.close()
conn.close()
print("✅ Deployment completed successfully")
