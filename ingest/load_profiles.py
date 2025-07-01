import pandas as pd
import duckdb
import os

# Define paths
DATA_DIR = "data"
CSV_FILE = os.path.join(DATA_DIR, "structured", "users.csv")
DB_FILE = os.path.join(DATA_DIR, "db", "profiles.duckdb")

# Ensure parent directory for DB exists
db_dir = os.path.dirname(DB_FILE)
if not os.path.exists(db_dir):
    os.makedirs(db_dir)

# Connect to DuckDB (it will create the file if it doesn't exist)
con = duckdb.connect(database=DB_FILE, read_only=False)

# Ingest data using DuckDB's native CSV reader for robustness
try:
    # Drop the table if it exists to ensure a fresh start
    con.execute("DROP TABLE IF EXISTS users")
    
    # Use DuckDB's native CSV reader to create the table. This is more reliable.
    con.execute(f"CREATE TABLE users AS SELECT * FROM read_csv_auto('{CSV_FILE}')")
    
    print(f"Successfully created 'users' table in DuckDB from {CSV_FILE}")

    # Verify by querying all columns of the table to ensure correctness
    print("\nVerifying data in 'users' table (first 5 rows):")
    result = con.execute("SELECT * FROM users LIMIT 5").fetchdf()
    print(result)

except FileNotFoundError:
    print(f"Error: {CSV_FILE} not found. Please ensure it exists.")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    # Close the connection
    con.close()
