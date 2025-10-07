"""
Supabase data access layer with connection pooling
"""

import pandas as pd
import psycopg2
from psycopg2 import pool
from datetime import date
from typing import List, Dict
from flask import current_app

class SupabaseClient:
    def __init__(self, minconn=1, maxconn=5):
        self.pool = None
        self.minconn = minconn
        self.maxconn = maxconn
        self._connect_pool()

    # def _connect(self):
    #     """Establish connection to Supabase (PostgreSQL)"""
    #     try:
    #         self.connection = psycopg2.connect(
    #             host=current_app.config['DB_HOST'],
    #             database=current_app.config['DB_NAME'],
    #             user=current_app.config['DB_USER'],
    #             password=current_app.config['DB_PASSWORD'],
    #             port=current_app.config.get('DB_PORT', 6543)
    #         )
    #         print("✓ Connected to Supabase PostgreSQL successfully")
    #     except Exception as e:
    #         print(f"✗ Failed to connect to Supabase: {str(e)}")
    #         raise

    def _connect_pool(self):
        """Establish connection pool to Supabase (PostgreSQL)"""
        try:
            self.pool = psycopg2.pool.SimpleConnectionPool(
                self.minconn,
                self.maxconn,
                host=current_app.config['DB_HOST'],
                database=current_app.config['DB_NAME'],
                user=current_app.config['DB_USER'],
                password=current_app.config['DB_PASSWORD'],
                port=current_app.config.get('DB_PORT', 6543)
            )
            if self.pool:
                print("✓ Connection pool created successfully")
        except Exception as e:
            print(f"✗ Failed to create connection pool: {str(e)}")
            raise

    def _get_conn(self):
        """Get a connection from the pool"""
        if self.pool:
            return self.pool.getconn()
        else:
            raise Exception("Connection pool is not initialized")

    def _release_conn(self, conn):
        """Return a connection to the pool"""
        if self.pool and conn:
            self.pool.putconn(conn)

    def fetch_price_data(self, tickers: List[str], start_date: date, end_date: date) -> pd.DataFrame:
        """Fetch historical price data from Supabase table `daily_prices`"""
        conn = self._get_conn()
        try:
            ticker_list = "', '".join(tickers)
            query = f"""
            SELECT date, ticker, price
            FROM daily_prices
            WHERE ticker IN ('{ticker_list}')
            AND date >= '{start_date}'
            AND date <= '{end_date}'
            ORDER BY date, ticker
            """
            df = pd.read_sql(query, conn)
            if df.empty:
                return pd.DataFrame()

            price_df = df.pivot(index='date', columns='ticker', values='price')
            price_df.index = pd.to_datetime(price_df.index)
            price_df.index.name = 'Date'
            price_df = price_df.ffill()
            return price_df
        finally:
            self._release_conn(conn)

    def get_fund_allocations(self, fund_id: str) -> Dict[str, float]:
        conn = self._get_conn()
        try:
            query = f"""
            SELECT ticker, weight
            FROM fund_allocations
            WHERE fund_id = '{fund_id}'
            """
            df = pd.read_sql(query, conn)
            return {row['ticker']: float(row['weight']) for _, row in df.iterrows()}
        finally:
            self._release_conn(conn)

    def get_available_tickers(self) -> List[str]:
        conn = self._get_conn()
        try:
            query = "SELECT DISTINCT ticker FROM daily_prices ORDER BY ticker"
            df = pd.read_sql(query, conn)
            return df['ticker'].tolist()
        finally:
            self._release_conn(conn)

    def get_date_range(self) -> tuple:
        conn = self._get_conn()
        try:
            query = "SELECT MIN(date) as min_date, MAX(date) as max_date FROM daily_prices"
            df = pd.read_sql(query, conn)
            if not df.empty:
                return df['min_date'][0], df['max_date'][0]
            return None, None
        finally:
            self._release_conn(conn)

    def close_pool(self):
        if self.pool:
            self.pool.closeall()
            print("✓ Connection pool closed")

# Global instance
supabase_client = None

def get_supabase_client() -> SupabaseClient:
    global supabase_client
    if supabase_client is None:
        supabase_client = SupabaseClient()
    return supabase_client
