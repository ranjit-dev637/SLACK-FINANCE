import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

try:
    conn = psycopg2.connect(user="postgres", password="postgres", host="localhost", port="5432")
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM pg_database WHERE datname='finance_db'")
    if not cursor.fetchone():
        cursor.execute("CREATE DATABASE finance_db;")
        print("Database 'finance_db' created successfully.")
    else:
        print("Database 'finance_db' already exists.")
    cursor.close()
    conn.close()
except Exception as e:
    print(f"Error: {e}")
