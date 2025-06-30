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

# Read data from CSV
try:
    profiles = pd.read_csv(CSV_FILE)
    print(f"Successfully loaded {len(profiles)} profiles from {CSV_FILE}")

    # Create a table and insert data
    con.execute("DROP TABLE IF EXISTS users")
    con.execute("CREATE TABLE users AS SELECT * FROM profiles")
    print("Successfully created 'users' table in DuckDB.")

    # Verify by querying the table
    print("\nVerifying data in 'users' table:")
    result = con.execute("SELECT name, company, school FROM users LIMIT 5").fetchdf()
    print(result)

except FileNotFoundError:
    print(f"Error: {CSV_FILE} not found. Please create it with user data.")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    # Close the connection
    con.close()
