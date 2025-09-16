import os
import glob
import snowflake.connector

# Connect using GitHub secrets
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

# Explicitly set DB + Schema context
db = os.getenv("SNOW_DATABASE")
schema = os.getenv("SNOW_SCHEMA")

print("▶ Current working dir:", os.getcwd())
print(f"▶ Using Database: {db}, Schema: {schema}")

cur.execute(f"USE DATABASE {db}")
cur.execute(f"USE SCHEMA {schema}")

# 1. Ensure stage exists
print("▶ Ensuring stage 'repo_stage' exists...")
cur.execute("""
CREATE STAGE IF NOT EXISTS repo_stage
FILE_FORMAT = (TYPE=CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"')
""")

# 2. Upload CSV files
csv_dir = os.path.join(os.getcwd(), "data", "csv")
print("▶ Looking for CSVs in:", csv_dir)

if not os.path.exists(csv_dir):
    print("⚠️ CSV directory does not exist!")
else:
    csv_files = glob.glob(os.path.join(csv_dir, "*.csv"))
    if not csv_files:
        print("⚠️ No CSV files found in data/csv/")
    else:
        for file in csv_files:
            abs_path = os.path.abspath(file)
            filename = os.path.basename(file)
            print(f"▶ Uploading {abs_path} → {filename}.gz")
            try:
                cur.execute(
                    f"PUT file://{abs_path} @repo_stage AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
                )
            except Exception as e:
                print(f"❌ Failed to upload {file}: {e}")
                raise

# 3. Verify stage contents
print("▶ Files in repo_stage:")
cur.execute("LIST @repo_stage")
for row in cur.fetchall():
    print("   ", row)

# 4. Run SQL scripts from repo folders
folders = ["migrations", "tables", "constraints", "data", "types",
           "functions", "procedures", "views", "scripts"]

for folder in folders:
    if os.path.isdir(folder) and not folder.endswith("admin_only"):
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
