import sys
import os

# Add the election_data_pipeline directory to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), 'election_data_pipeline'))

from sqlalchemy import create_engine, text
from config.settings import settings

def run_schema():
    print(f"Connecting to database: {settings.DB_DATABASE} at {settings.DB_HOST}")
    
    # Connect to MySQL server (without database first to create it if needed, 
    # but settings has DB_DATABASE. Let's assume DB exists or we try to connect to server)
    # Actually sqlalchemy URL includes DB. If DB doesn't exist, it might fail.
    # Let's try connecting to just the server first to create DB if not exists.
    
    base_url = settings.DATABASE_URL.rsplit('/', 1)[0]
    db_name = settings.DB_DATABASE
    
    engine_server = create_engine(base_url)
    
    try:
        with engine_server.connect() as conn:
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{db_name}`"))
            print(f"Database {db_name} ensured.")
    except Exception as e:
        print(f"Warning checking database existence: {e}")
        
    # Now connect to the specific DB and run schema
    engine = create_engine(settings.DATABASE_URL)
    
    # Path is relative to this script: python/create_tables.py -> python/schema.sql
    schema_path = os.path.join(os.path.dirname(__file__), 'schema.sql')
    
    with open(schema_path, 'r') as f:
        schema_sql = f.read()
        
    statements = schema_sql.split(';')
    
    with engine.connect() as conn:
        for statement in statements:
            if statement.strip():
                try:
                    conn.execute(text(statement))
                    conn.commit()
                except Exception as e:
                    print(f"Error executing statement: {statement[:50]}... \nError: {e}")

    print("Schema executed successfully.")

if __name__ == "__main__":
    run_schema()
