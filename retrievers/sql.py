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
    recommendations = [{
        "user_id": row[0],
        "reason": f"Same {field}: {value}"
    } for row in result]
    return recommendations

def get_user_details(user_ids: list[str]):
    """Fetches full details for a list of user IDs from DuckDB by creating its own connection."""
    if not user_ids:
        return []

    con = None
    try:
        con = duckdb.connect(database=DUCKDB_PATH, read_only=True)
        
        # Create a placeholder string for the IN clause
        placeholders = ', '.join(['?'] * len(user_ids))
        
        query = f"SELECT user_id, name, email, company, school, location, bio, title FROM users WHERE user_id IN ({placeholders})"
        
        cursor = con.execute(query, user_ids)
        results = cursor.fetchall()
        
        # Convert results to a list of dictionaries using column names for robustness
        users_details = []
        columns = [desc[0] for desc in cursor.description]
        for row in results:
            users_details.append(dict(zip(columns, row)))
            
        return users_details
    finally:
        if con:
            con.close()

def get_user_id_by_name(name: str):
    """Fetches a user_id for a given user name from DuckDB by creating its own connection."""
    con = None
    try:
        con = duckdb.connect(database=DUCKDB_PATH, read_only=True)
        query = "SELECT user_id FROM users WHERE name = ? LIMIT 1"
        result = con.execute(query, [name]).fetchone()
        return result[0] if result else None
    finally:
        if con:
            con.close()

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
