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
