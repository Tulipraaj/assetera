from dotenv import load_dotenv
import os
from datetime import timedelta

load_dotenv()

class Config:
    # Flask config
    SECRET_KEY = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')
    SQLALCHEMY_DATABASE_URI = os.environ.get('SQLALCHEMY_DATABASE_URI')  # Supabase Postgres URL
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    PERMANENT_SESSION_LIFETIME = timedelta(days=7)

    # Snowflake credentials
    SNOWFLAKE_USER = os.environ.get('SNOWFLAKE_USER')
    SNOWFLAKE_PASSWORD = os.environ.get('SNOWFLAKE_PASSWORD')
    SNOWFLAKE_ACCOUNT = os.environ.get('SNOWFLAKE_ACCOUNT')
    SNOWFLAKE_WAREHOUSE = os.environ.get('SNOWFLAKE_WAREHOUSE')
    SNOWFLAKE_DATABASE = os.environ.get('SNOWFLAKE_DATABASE')
    SNOWFLAKE_SCHEMA = os.environ.get('SNOWFLAKE_SCHEMA')
    SNOWFLAKE_ROLE = os.environ.get('SNOWFLAKE_ROLE')

    DB_HOST = os.getenv("DB_HOST", "db.<your-supabase-host>.supabase.co")
    DB_NAME = os.getenv("DB_NAME", "postgres")
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "your_password")
    DB_PORT = os.getenv("DB_PORT", "6543")  # usually 6543 for Supabase
