import os
import glob
import snowflake.connector

# Fetch env variables set by GitHub Actions
DB = os.getenv("SNOW_DATABASE")
SCHEMA = os.getenv("SNOW_SCHEMA")
ACCOUNT = os.getenv("SNOW_ACCOUNT")
USER = os.getenv("SNOW_USER")
PASSWORD = os.getenv("SNOW_PASSWORD")
ROLE = os.getenv("SNOW_ROLE")
WAREHOUSE = os.getenv("SNOW_WAREHOUSE")

print(f"▶ Current working dir: {os.getcwd()}")
print(f"▶ Deploying to Database: {DB}, Schema: {SCHEMA}")

# Connect to Snowflake
conn = snowflake.connector.connect(
    account=ACCOUNT,
    user=USER,
    password=PASSWORD,
    role=ROLE,
    warehouse=WAREHOUSE,
    database=DB,
    schema=SCHEMA
)
cur = conn.cursor()

# Ensure the DB/Schema context is explicitly set
cur.execute(f"USE DATABASE {DB}")
cur.execute(f"USE SCHEMA {SCHEMA}")

# 1. Ensure stage exists
print("▶ Ensuring stage 'repo_stage' exists...")
cur.execute(f"""
CREATE STAGE IF NOT EXISTS {DB}.{SCHEMA}.repo_stage
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
                    f"PUT file://{abs_path} @{DB}.{SCHEMA}.repo_stage AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
                )
            except Exception as e:
                print(f"❌ Failed to upload {file}: {e}")
                raise

# 3. Verify stage contents
print("▶ Files in repo_stage:")
cur.execute(f"LIST @{DB}.{SCHEMA}.repo_stage")
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
