import sqlite3
import os
import pandas as pd
from src.ddl_generator import generate_ddl
from src.exporter import Exporter

def run_tests():
    print("Running DDL Generator Tests...")
    schema_path = "src/schema.json"
    output_dir = "output_data"
    
    # 1. Generate schemas
    dialects = ["postgres", "mysql", "snowflake", "sqlite"]
    for dialect in dialects:
        generate_ddl(schema_path, output_dir, dialect)
        
    print("\nRunning Exporter Tests...")
    
    # 2. Test Exporter SQL Inserts
    # Create dummy dataframe
    data = {
        "user_id": ["uuid-1", "uuid-2"],
        "registration_date": ["2023-01-01 10:00:00", "2023-01-02 11:30:00"],
        "subscription_status": ["active", "inactive"],
        "age": [28, 35],
        "first_name": ["John O'Connor", "Jane"] # Testing string escaping
    }
    df = pd.DataFrame(data)
    
    exporter = Exporter(output_dir)
    sql_file = exporter.to_sql_inserts(df, "users", "postgres")
    
    print("\nValidating SQLite DB creation...")
    # 3. Test executing SQLite script
    sqlite_schema_path = os.path.join(output_dir, "schema_sqlite.sql")
    
    if os.path.exists(sqlite_schema_path):
        conn = sqlite3.connect(":memory:")
        cursor = conn.cursor()
        
        with open(sqlite_schema_path, "r") as f:
            sql_script = f.read()
            
        try:
            cursor.executescript(sql_script)
            print("Successfully executed SQLite schema script in-memory!")
            
            # Verify tables exist
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            print(f"Found {len(tables)} tables created in SQLite.")
            
        except Exception as e:
            print(f"Failed to execute SQLite script: {e}")
            
        finally:
            conn.close()

if __name__ == "__main__":
    run_tests()
