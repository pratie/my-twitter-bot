import psycopg2
import pandas as pd
from datetime import datetime
import sys

# Import your DB_CONFIG here
# from your_config import DB_CONFIG

def get_db_connection():
    """Establish connection to PostgreSQL database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise

def create_schema():
    """Create the field_prompts table and indexes"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS field_prompts (
            id SERIAL PRIMARY KEY,
            area VARCHAR(100) NOT NULL,
            sub_area VARCHAR(100) NOT NULL,
            field VARCHAR(200) NOT NULL,
            prompt TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(area, sub_area, field)
        );
        """
        
        cursor.execute(create_table_query)
        
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_field_prompts_area_subarea ON field_prompts(area, sub_area);",
            "CREATE INDEX IF NOT EXISTS idx_field_prompts_field ON field_prompts(field);",
            "CREATE INDEX IF NOT EXISTS idx_field_prompts_updated_at ON field_prompts(updated_at);"
        ]
        
        for index_query in indexes:
            cursor.execute(index_query)
        
        conn.commit()
        print("Schema created successfully")
        
    except Exception as e:
        conn.rollback()
        print(f"Error creating schema: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def preview_ingestion(file_path):
    """Preview what records would be new vs existing before actual ingestion"""
    try:
        df = pd.read_excel(file_path)
        
        expected_columns = ['Area', 'Sub Area', 'Field', 'Prompt']
        if not all(col in df.columns for col in expected_columns):
            print(f"Missing columns. Expected: {expected_columns}")
            return False
        
        df = df.dropna(subset=['Area', 'Sub Area', 'Field'])
        df = df.drop_duplicates(subset=['Area', 'Sub Area', 'Field'], keep='last')
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        new_count = 0
        existing_count = 0
        
        for _, row in df.iterrows():
            cursor.execute("""
                SELECT id FROM field_prompts 
                WHERE area = %s AND sub_area = %s AND field = %s
            """, (row['Area'].strip(), row['Sub Area'].strip(), row['Field'].strip()))
            
            if cursor.fetchone():
                existing_count += 1
            else:
                new_count += 1
        
        print(f"Preview: {new_count} new records, {existing_count} existing records")
        return True
        
    except Exception as e:
        print(f"Error previewing file: {e}")
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def check_existing_records():
    """Check how many records already exist"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM field_prompts;")
        count = cursor.fetchone()[0]
        return count
    except Exception as e:
        print(f"Error checking existing records: {e}")
        return 0
    finally:
        cursor.close()
        conn.close()

def ingest_excel_data(file_path):
    """Read Excel file and ingest data into database"""
    try:
        df = pd.read_excel(file_path)
        
        expected_columns = ['Area', 'Sub Area', 'Field', 'Prompt']
        if not all(col in df.columns for col in expected_columns):
            print(f"Missing columns. Expected: {expected_columns}")
            return False
        
        original_count = len(df)
        df = df.dropna(subset=['Area', 'Sub Area', 'Field'])
        df = df.fillna('')
        df = df.drop_duplicates(subset=['Area', 'Sub Area', 'Field'], keep='last')
        
        print(f"Processing {len(df)} records")
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        insert_query = """
        INSERT INTO field_prompts (area, sub_area, field, prompt, created_at, updated_at)
        VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT (area, sub_area, field) DO NOTHING
        """
        
        success_count = 0
        error_count = 0
        skipped_count = 0
        new_count = 0
        
        for index, row in df.iterrows():
            try:
                cursor.execute("""
                    SELECT id FROM field_prompts 
                    WHERE area = %s AND sub_area = %s AND field = %s
                """, (row['Area'].strip(), row['Sub Area'].strip(), row['Field'].strip()))
                
                existing_record = cursor.fetchone()
                
                cursor.execute(insert_query, (
                    row['Area'].strip(),
                    row['Sub Area'].strip(),
                    row['Field'].strip(),
                    str(row['Prompt']).strip() if pd.notna(row['Prompt']) else ''
                ))
                
                if existing_record:
                    skipped_count += 1
                else:
                    new_count += 1
                    
                success_count += 1
                
            except Exception as e:
                error_count += 1
                print(f"Error processing row {index}: {e}")
                conn.rollback()
                conn = get_db_connection()
                cursor = conn.cursor()
        
        conn.commit()
        
        print(f"Ingestion completed: {new_count} new, {skipped_count} skipped, {error_count} errors")
        return True
        
    except Exception as e:
        print(f"❌ Error reading Excel file: {e}")
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def get_database_stats():
    """Get database statistics"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM field_prompts;")
        total_records = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT area) FROM field_prompts;")
        unique_areas = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT sub_area) FROM field_prompts;")
        unique_sub_areas = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT COUNT(*) FROM field_prompts 
            WHERE updated_at >= CURRENT_DATE - INTERVAL '7 days'
        """)
        recent_updates = cursor.fetchone()[0]
        
        return {
            'total_records': total_records,
            'unique_areas': unique_areas,
            'unique_sub_areas': unique_sub_areas,
            'recent_updates': recent_updates
        }
        
    except Exception as e:
        print(f"Error getting database stats: {e}")
        return None
    finally:
        cursor.close()
        conn.close()

def main():
    """Main function to run data management operations"""
    print("Starting Data Management Operations")
    print("=" * 40)
    
    print("\n1. Creating Schema...")
    create_schema()
    
    print("\n2. Current Database Stats:")
    stats = get_database_stats()
    if stats:
        for key, value in stats.items():
            print(f"   {key.replace('_', ' ').title()}: {value}")
    
    excel_file = "Field_prompts.xlsx"
    print(f"\n3. Previewing ingestion for {excel_file}...")
    preview_success = preview_ingestion(excel_file)
    
    if preview_success:
        proceed = input("\nProceed with ingestion? (y/n): ").strip().lower()
        if proceed != 'y':
            print("Ingestion cancelled")
            return
    
    print(f"\n4. Starting data ingestion...")
    success = ingest_excel_data(excel_file)
    
    if success:
        print("\n5. Final Database Stats:")
        stats = get_database_stats()
        if stats:
            for key, value in stats.items():
                print(f"   {key.replace('_', ' ').title()}: {value}")
    
    print("\nData management operations completed")

if __name__ == "__main__":
    main()
