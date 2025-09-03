import os
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

# Run scripts in proper order
folders = ["migrations", "tables", "constraints", "data", "views", "scripts"]
for folder in folders:
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
