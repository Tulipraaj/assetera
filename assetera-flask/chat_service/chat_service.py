# chat_service.py
"""
Chat service for handling messages with Supabase (HTTP) or direct Postgres (psycopg).
- If SUPABASE_URL & SUPABASE_ANON_KEY are set -> use Supabase client.
- Else if DATABASE_URL is set -> use psycopg direct Postgres.
"""
import os
import logging
from typing import List, Optional, Dict

logger = logging.getLogger(__name__)

class ChatService:
    def __init__(self, supabase_url: Optional[str]=None, supabase_key: Optional[str]=None, pg_dsn: Optional[str]=None):
        self.mode = None
        self.supabase = None
        self.pg_conn = None

        supabase_url = (supabase_url or os.getenv("SUPABASE_URL", "")).strip()
        supabase_key = (supabase_key or os.getenv("SUPABASE_ANON_KEY", "")).strip()
        pg_dsn = (pg_dsn or os.getenv("DATABASE_URL", "")).strip()

        if supabase_url and supabase_key:
            # --- Supabase HTTP mode ---
            from supabase import create_client, Client  # import only if needed
            self.supabase: Client = create_client(supabase_url, supabase_key)
            self.mode = "supabase"
            logger.info("ChatService initialized in SUPABASE mode")
        elif pg_dsn:
            # --- Direct Postgres mode ---
            import psycopg2
            self.pg_conn = psycopg2.connect(pg_dsn, autocommit=True)
            self.mode = "postgres"
            logger.info("ChatService initialized in POSTGRES mode")
        else:
            raise RuntimeError(
                "No credentials provided. Set either (SUPABASE_URL & SUPABASE_ANON_KEY) or DATABASE_URL."
            )

    # ---------- helpers ----------
    def _row_to_dict(self, row) -> Dict:
        # For psycopg default cursor: row is a tuple in the same order as SELECT columns.
        # We'll SELECT explicit columns to keep order stable.
        keys = ["id","customer_id","advisor_id","sender","content","created_at","read"]
        return dict(zip(keys, row))

    # ---------- public API ----------
    def send_message(self, customer_id: int, advisor_id: str, sender: str, content: str) -> Optional[Dict]:
        if sender not in ("customer", "advisor"):
            raise ValueError("sender must be 'customer' or 'advisor'")

        if self.mode == "supabase":
            try:
                data = {
                    "customer_id": customer_id,
                    "advisor_id": advisor_id,
                    "sender": sender,
                    "content": content,  # correct column
                }
                resp = self.supabase.table("messages").insert(data).execute()
                return resp.data[0] if resp.data else None
            except Exception as e:
                logger.error(f"[SUPABASE] send_message error: {e}", exc_info=True)
                return None

        # postgres mode
        try:
            with self.pg_conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO public.messages (customer_id, advisor_id, sender, content)
                    VALUES (%s, %s::uuid, %s, %s)
                    RETURNING id, customer_id, advisor_id, sender, content, created_at, read;
                    """,
                    (customer_id, advisor_id, sender, content),
                )
                row = cur.fetchone()
                return self._row_to_dict(row) if row else None
        except Exception as e:
            logger.error(f"[POSTGRES] send_message error: {e}", exc_info=True)
            return None

    def get_messages(self, customer_id: int, advisor_id: str, limit: int = 100) -> List[Dict]:
        if self.mode == "supabase":
            try:
                resp = (
                    self.supabase.table("messages")
                    .select("*")
                    .eq("customer_id", customer_id)
                    .eq("advisor_id", advisor_id)
                    .order("created_at", desc=False)
                    .limit(limit)
                    .execute()
                )
                return resp.data or []
            except Exception as e:
                logger.error(f"[SUPABASE] get_messages error: {e}", exc_info=True)
                return []

        # postgres mode
        try:
            with self.pg_conn.cursor() as cur:
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
                return [self._row_to_dict(r) for r in rows]
        except Exception as e:
            logger.error(f"[POSTGRES] get_messages error: {e}", exc_info=True)
            return []

    def get_latest_message(self, customer_id: int, advisor_id: str) -> Optional[Dict]:
        if self.mode == "supabase":
            try:
                resp = (
                    self.supabase.table("messages")
                    .select("*")
                    .eq("customer_id", customer_id)
                    .eq("advisor_id", advisor_id)
                    .order("created_at", desc=True)
                    .limit(1)
                    .execute()
                )
                return resp.data[0] if (resp.data and len(resp.data)) else None
            except Exception as e:
                logger.error(f"[SUPABASE] get_latest_message error: {e}", exc_info=True)
                return None

        # postgres mode
        try:
            with self.pg_conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, customer_id, advisor_id, sender, content, created_at, read
                    FROM public.messages
                    WHERE customer_id = %s AND advisor_id = %s::uuid
                    ORDER BY created_at DESC
                    LIMIT 1;
                    """,
                    (customer_id, advisor_id),
                )
                row = cur.fetchone()
                return self._row_to_dict(row) if row else None
        except Exception as e:
            logger.error(f"[POSTGRES] get_latest_message error: {e}", exc_info=True)
            return None
