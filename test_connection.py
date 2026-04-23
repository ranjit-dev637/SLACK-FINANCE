"""
test_connection.py
------------------
Run once to verify the direct Supabase PostgreSQL connection.

Usage:
    python test_connection.py
"""

import os
import sys
from dotenv import load_dotenv

# ── 1. Load env vars ──────────────────────────────────────────────────────────
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("[ERR]  DATABASE_URL is not set. Check your .env file.")
    sys.exit(1)

print(f"[OK]   DATABASE_URL loaded: {DATABASE_URL[:50]}...")   # truncated for safety

# ── 2. Test with psycopg2 (raw connection) ────────────────────────────────────
try:
    import psycopg2
except ImportError:
    print("[ERR]  psycopg2 is not installed. Run: pip install psycopg2-binary")
    sys.exit(1)

try:
    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor()
    cur.execute("SELECT version();")
    version = cur.fetchone()[0]
    cur.close()
    conn.close()
    print(f"[OK]   psycopg2 connected successfully!")
    print(f"       Server: {version}")
except Exception as e:
    print(f"[ERR]  psycopg2 connection failed: {e}")
    sys.exit(1)

# ── 3. Test with SQLAlchemy (ORM layer) ───────────────────────────────────────
try:
    from sqlalchemy import create_engine, text
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    with engine.connect() as connection:
        result = connection.execute(text("SELECT current_database(), current_user;"))
        db_name, db_user = result.fetchone()
    print(f"[OK]   SQLAlchemy connected | database={db_name} | user={db_user}")
except Exception as e:
    print(f"[ERR]  SQLAlchemy connection failed: {e}")
    sys.exit(1)

print("\n[DONE] All checks passed. Your database connection is working correctly.")
