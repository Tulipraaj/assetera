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

# Connect to Snowflake WITHOUT specifying database/schema
print("▶ Connecting to Snowflake...")
conn = snowflake.connector.connect(
    account=ACCOUNT,
    user=USER,
    password=PASSWORD,
    role=ROLE,
    warehouse=WAREHOUSE
    # Don't specify database and schema here!
)

cur = conn.cursor()

# Debug: Show current context and available databases
cur.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()")
result = cur.fetchone()
print(f"▶ Connected as: User={result[0]}, Role={result[1]}, Warehouse={result[2]}")

# List available databases for debugging
print("▶ Available databases:")
cur.execute("SHOW DATABASES")
databases = cur.fetchall()
for db in databases:
    print(f"   - {db[1]}")

# Check if our target database exists (with better debugging)
available_db_names = [db[1] for db in databases]
print(f"▶ Target database: '{DB}' (length: {len(DB)})")
print(f"▶ Available databases with lengths:")
for db_name in available_db_names:
    print(f"   - '{db_name}' (length: {len(db_name)})")

# Strip whitespace and compare case-insensitively
DB_clean = DB.strip().upper()
target_db_exists = any(db_name.strip().upper() == DB_clean for db_name in available_db_names)

if not target_db_exists:
    print(f"❌ Database '{DB}' not found in available databases!")
    print(f"▶ Cleaned target: '{DB_clean}'")
    print(f"▶ Cleaned available: {[db.strip().upper() for db in available_db_names]}")
    raise Exception(f"Database '{DB}' does not exist or is not accessible")

print(f"✅ Database '{DB}' found in available databases")

# Now set the database and schema context
print(f"▶ Setting context to database: {DB}")
cur.execute(f"USE DATABASE {DB}")

print(f"▶ Setting context to schema: {SCHEMA}")
try:
    cur.execute(f"USE SCHEMA {SCHEMA}")
except Exception as e:
    print(f"⚠️ Schema '{SCHEMA}' might not exist. Creating it...")
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {DB}.{SCHEMA}")
    cur.execute(f"USE SCHEMA {SCHEMA}")

# Verify final context
cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
result = cur.fetchone()
print(f"▶ Current context: Database={result[0]}, Schema={result[1]}")

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
        print(f"▶ Processing folder: {folder}")
        sql_files = [f for f in sorted(os.listdir(folder)) if f.endswith(".sql")]
        if not sql_files:
            print(f"   No SQL files found in {folder}")
            continue
            
        for file in sql_files:
            path = os.path.join(folder, file)
            print(f"▶ Running {path}")
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    sql = f.read()
                    
                # Split and execute statements
                statements = [stmt.strip() for stmt in sql.split(";") if stmt.strip()]
                for i, stmt in enumerate(statements):
                    try:
                        cur.execute(stmt)
                        print(f"   ✅ Statement {i+1}/{len(statements)} executed")
                    except Exception as e:
                        print(f"❌ Error in {file}, statement {i+1}: {e}")
                        print(f"   Statement: {stmt[:100]}...")
                        raise
                        
            except Exception as e:
                print(f"❌ Error processing {file}: {e}")
                raise

cur.close()
conn.close()
print("✅ Deployment completed successfully")
