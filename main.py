
def print_tables():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        
        # Query to get all table names in the current database
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        
        tables = cursor.fetchall()
        
        print("Tables in the database:")
        for table in tables:
            print(f"- {table[0]}")
            
    except Exception as e:
        print(f"Error fetching tables: {e}")
    finally:
        cursor.close()
        conn.close()

# Call the function
print_tables()
