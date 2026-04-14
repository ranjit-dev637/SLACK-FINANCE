from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the exact, raw Supabase database connection string from the environment
raw_db_url = os.getenv("DATABASE_URL")
if not raw_db_url:
    raise ValueError("DATABASE_URL environment variable is not set. Please check your .env file.")

# Safely construct the SQLAlchemy URL in-memory.
# We replace the literal password substring with its URL-encoded equivalent
# so SQLAlchemy's engine parser does not split the URL at the wrong @ symbol.
# This ensures it passes the exact "Supabase@8637" to psycopg2 automatically.
if "Supabase@8637" in raw_db_url:
    safe_db_url = raw_db_url.replace("Supabase@8637", "Supabase%408637")
else:
    safe_db_url = raw_db_url

engine = create_engine(safe_db_url)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
