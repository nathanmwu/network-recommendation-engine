import os
import duckdb
import argparse

# --- Configuration ---
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
DUCKDB_PATH = os.path.join(DATA_DIR, 'db', 'profiles.duckdb')

def get_sql_recommendations(field: str, value: str, duckdb_con):
    """Finds users based on a specific field and value in DuckDB."""
    # Basic validation to prevent SQL injection, though parameters are safer
    allowed_fields = ["company", "school", "location"]
    if field not in allowed_fields:
        raise ValueError(f"Invalid field. Allowed fields are: {', '.join(allowed_fields)}")

    query = f"""SELECT user_id FROM users WHERE {field} = ?"""
    result = duckdb_con.execute(query, [value]).fetchall()
    
    # fetchall returns a list of tuples, e.g., [('user2',), ('user5',)]
    recommendations = [row[0] for row in result]
    return recommendations

def main():
    """Main function to test the SQL retriever."""
    parser = argparse.ArgumentParser(description="Get SQL-based recommendations for a user.")
    parser.add_argument("--field", type=str, required=True, choices=["company", "school", "location"], help="The field to search on.")
    parser.add_argument("--value", type=str, required=True, help="The value to search for.")
    args = parser.parse_args()

    con = None
    try:
        con = duckdb.connect(database=DUCKDB_PATH, read_only=True)
        print(f"Successfully connected to DuckDB.")

        recommendations = get_sql_recommendations(args.field, args.value, con)
        
        print(f"\n--- SQL Recommendations for {args.field} = '{args.value}' ---")
        if recommendations:
            for rec in recommendations:
                print(f"- {rec}")
        else:
            print("No users found matching the criteria.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if con:
            con.close()

if __name__ == "__main__":
    main()
