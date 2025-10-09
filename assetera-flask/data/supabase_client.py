"""
Supabase data access layer with connection pooling (dual DB: core + chat)
- Core pool:  used by your existing analytics methods (prices, allocations, etc.)
- Chat pool:  used ONLY for chat tables (public.messages, public.advisors)
If CHAT_* config is missing, chat pool falls back to core pool.

Supports either discrete params (HOST/NAME/USER/PASSWORD/PORT) or a DSN URL.
Recommended: sslmode='require' for Supabase.
"""

import os
import pandas as pd
import psycopg2
import psycopg2.extras as extras
from psycopg2 import pool
from datetime import date
from typing import List, Dict, Optional
from flask import current_app
from supabase import create_client, Client  # keep if you use it elsewhere

class SupabaseClient:
    def __init__(self, minconn=1, maxconn=5):
        self.minconn = minconn
        self.maxconn = maxconn
        self.core_pool: Optional[pool.SimpleConnectionPool] = None
        self.chat_pool: Optional[pool.SimpleConnectionPool] = None

        # Optional Supabase HTTP client (keep if you use it elsewhere)
        sb_url = current_app.config.get('SUPABASE_URL') or os.getenv('SUPABASE_URL')
        sb_key = current_app.config.get('SUPABASE_ANON_KEY') or os.getenv('SUPABASE_ANON_KEY')
        self.supabase: Optional[Client] = None
        if sb_url and sb_key:
            self.supabase = create_client(sb_url, sb_key)

        self._connect_core_pool()
        self._connect_chat_pool()  # falls back to core if chat creds absent

    # ---------- Pool builders ----------
    def _connect_core_pool(self):
        dsn = (
            current_app.config.get('DATABASE_URL')
            or os.getenv('DATABASE_URL')
        )
        if dsn:
            self.core_pool = psycopg2.pool.SimpleConnectionPool(
                self.minconn, self.maxconn, dsn=dsn
            )
            print("✓ Core pool created (DSN)")
            return

        host = current_app.config.get('DB_HOST') or os.getenv('DB_HOST')
        database = current_app.config.get('DB_NAME') or os.getenv('DB_NAME', 'postgres')
        user = current_app.config.get('DB_USER') or os.getenv('DB_USER', 'postgres')
        password = current_app.config.get('DB_PASSWORD') or os.getenv('DB_PASSWORD')
        port = int(current_app.config.get('DB_PORT') or os.getenv('DB_PORT', 6543))
        if not (host and password):
            raise RuntimeError("Core DB config missing (DB_HOST/DB_PASSWORD or DATABASE_URL)")

        self.core_pool = psycopg2.pool.SimpleConnectionPool(
            self.minconn, self.maxconn,
            host=host, database=database, user=user, password=password, port=port,
            sslmode='require', connect_timeout=10
        )
        print("✓ Core pool created")

    def _connect_chat_pool(self):
        # Prefer DSN first
        dsn = (
            current_app.config.get('CHAT_DATABASE_URL')
            or os.getenv('CHAT_DATABASE_URL')
        )
        if dsn:
            self.chat_pool = psycopg2.pool.SimpleConnectionPool(
                self.minconn, self.maxconn, dsn=dsn
            )
            print("✓ Chat pool created (DSN)")
            return

        # Otherwise discrete params
        host = current_app.config.get('CHAT_DB_HOST') or os.getenv('CHAT_DB_HOST')
        database = current_app.config.get('CHAT_DB_NAME') or os.getenv('CHAT_DB_NAME')
        user = current_app.config.get('CHAT_DB_USER') or os.getenv('CHAT_DB_USER')
        password = current_app.config.get('CHAT_DB_PASSWORD') or os.getenv('CHAT_DB_PASSWORD')
        port = current_app.config.get('CHAT_DB_PORT') or os.getenv('CHAT_DB_PORT')

        # If chat creds not provided, fall back to core
        if not host:
            self.chat_pool = self.core_pool
            print("ℹ️  Chat pool not configured; using core pool for chat")
            return

        self.chat_pool = psycopg2.pool.SimpleConnectionPool(
            self.minconn, self.maxconn,
            host=host,
            database=database or 'postgres',
            user=user or 'postgres',
            password=password,
            port=int(port or 6543),
            sslmode='require',
            connect_timeout=10
        )
        print("✓ Chat pool created")

    # ---------- Pool helpers ----------
    def _get_conn(self, which: str = 'core'):
        pool_obj = self.chat_pool if which == 'chat' else self.core_pool
        if not pool_obj:
            raise RuntimeError(f"{which} pool is not initialized")
        return pool_obj.getconn()

    def _release_conn(self, conn, which: str = 'core'):
        pool_obj = self.chat_pool if which == 'chat' else self.core_pool
        if pool_obj and conn:
            pool_obj.putconn(conn)

    # ---------- Existing analytics methods (use CORE pool) ----------
    def fetch_price_data(self, tickers: List[str], start_date: date, end_date: date) -> pd.DataFrame:
        conn = self._get_conn('core')
        try:
            ticker_list = "', '".join(tickers)
            query = f"""
                SELECT date, ticker, price
                FROM daily_prices
                WHERE ticker IN ('{ticker_list}')
                  AND date >= %s
                  AND date <= %s
                ORDER BY date, ticker
            """
            df = pd.read_sql(query, conn, params=[start_date, end_date])
            if df.empty:
                return pd.DataFrame()
            price_df = df.pivot(index='date', columns='ticker', values='price')
            price_df.index = pd.to_datetime(price_df.index)
            price_df.index.name = 'Date'
            price_df = price_df.ffill()
            return price_df
        finally:
            self._release_conn(conn, 'core')

    def get_fund_allocations(self, fund_id: str) -> Dict[str, float]:
        conn = self._get_conn('core')
        try:
            query = "SELECT ticker, weight FROM fund_allocations WHERE fund_id = %s"
            df = pd.read_sql(query, conn, params=[fund_id])
            return {row['ticker']: float(row['weight']) for _, row in df.iterrows()}
        finally:
            self._release_conn(conn, 'core')

    def get_available_tickers(self) -> List[str]:
        conn = self._get_conn('core')
        try:
            query = "SELECT DISTINCT ticker FROM daily_prices ORDER BY ticker"
            df = pd.read_sql(query, conn)
            return df['ticker'].tolist()
        finally:
            self._release_conn(conn, 'core')

    def get_date_range(self) -> tuple:
        conn = self._get_conn('core')
        try:
            query = "SELECT MIN(date) as min_date, MAX(date) as max_date FROM daily_prices"
            df = pd.read_sql(query, conn)
            if not df.empty:
                return df['min_date'][0], df['max_date'][0]
            return None, None
        finally:
            self._release_conn(conn, 'core')

    # ---------- CHAT methods (always use CHAT pool) ----------
    def insert_message(self, customer_id: int, advisor_id: str, sender: str, content: str) -> Optional[Dict]:
        if sender not in ("customer", "advisor"):
            raise ValueError("sender must be 'customer' or 'advisor'")
        conn = self._get_conn('chat')
        try:
            with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO public.messages (customer_id, advisor_id, sender, content)
                    VALUES (%s, %s::uuid, %s, %s)
                    RETURNING id, customer_id, advisor_id, sender, content, created_at, read;
                    """,
                    (customer_id, advisor_id, sender, content),
                )
                row = cur.fetchone()
            conn.commit()
            return dict(row) if row else None
        except Exception as e:
            conn.rollback()
            print(f"[insert_message] error: {e}")
            return None
        finally:
            self._release_conn(conn, 'chat')

    def fetch_messages(self, customer_id: int, advisor_id: str, limit: int = 100) -> List[Dict]:
        conn = self._get_conn('chat')
        try:
            with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, customer_id, advisor_id, sender, content, created_at, read
                    FROM public.messages
                    WHERE customer_id = %s AND advisor_id = %s::uuid
                    ORDER BY created_at ASC
                    LIMIT %s;
                    """,
                    (customer_id, advisor_id, limit),
                )
                rows = cur.fetchall()
            return [dict(r) for r in rows]
        except Exception as e:
            print(f"[fetch_messages] error: {e}")
            return []
        finally:
            self._release_conn(conn, 'chat')

    # ---------- teardown ----------
    def close_pool(self):
        if self.core_pool:
            self.core_pool.closeall()
        if self.chat_pool and (self.chat_pool is not self.core_pool):
            self.chat_pool.closeall()
        print("✓ Pools closed")


# Global instance
supabase_client = None

def get_supabase_client() -> SupabaseClient:
    global supabase_client
    if supabase_client is None:
        supabase_client = SupabaseClient()
    return supabase_client
