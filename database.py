from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
import os
from dotenv import load_dotenv, find_dotenv

# Load environment variables from .env BEFORE reading any variable.
# - find_dotenv() locates the .env file regardless of the current working directory.
# - override=True ensures .env values always win over VS Code's environment injection.
load_dotenv(find_dotenv(), override=True)

# Validate DATABASE_URL is present
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL is not set. Check your .env file.")

# Debug print — confirms the correct URL is in use at startup (truncated for safety)
print(f"[database.py] DATABASE_URL loaded: {DATABASE_URL[:55]}...")

# Direct Supabase connection (port 5432) — no pooler, no encoding needed
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,   # drops stale connections before use
    pool_size=5,          # keep up to 5 persistent connections
    max_overflow=10,      # allow up to 10 extra connections under load
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
